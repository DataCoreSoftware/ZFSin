/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
 * or http://www.opensolaris.org/os/licensing.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL HEADER in each
 * file and include the License file at usr/src/OPENSOLARIS.LICENSE.
 * If applicable, add the following below this CDDL HEADER, with the
 * fields enclosed by brackets "[]" replaced with your own identifying
 * information: Portions Copyright [yyyy] [name of copyright owner]
 *
 * CDDL HEADER END
 */

/*
 *
 * Copyright (C) 2017,2019 Jorgen Lundman <lundman@lundman.net>
 *
 */

 /*
  * Implementation details. 
  * Using SynchronizationEvent that autoresets. When in 'Signaled' 
  * state the mutex is considered FREE/Available to be locked.
  * Call KeWaitForSingleObject() to wait for it to be made
  * 'available' (either blocking, or polling for *Try method)
  * Calling KeSetEvent() sets event to Signaled, and wakes 'one'
  * waiter, before Clearing it again.
  * We attempt to avoid calling KeWaitForSingleObject() by
  * using atomic CAS on m_owner, in the simple cases.
  */

#include <sys/mutex.h>
#include <string.h>
#include <sys/debug.h>
#include <sys/thread.h>
#include <sys/types.h>
#include <fltkernel.h>

uint64_t zfs_active_mutex = 0;

#define MUTEX_INITIALISED 0x23456789
#define MUTEX_DESTROYED 0x98765432


int spl_mutex_subsystem_init(void)
{
	return 0;
}

void spl_mutex_subsystem_fini(void)
{

}

void spl_mutex_init(kmutex_t *mp, char *name, kmutex_type_t type, void *ibc)
{
	(void)name;
	ASSERT(ibc == NULL);

	if (mp->initialised == MUTEX_INITIALISED)
		panic("%s: mutex already initialised\n", __func__);
	mp->initialised = MUTEX_INITIALISED;
	mp->set_event_guard = 0;
	mp->m_owner = NULL;

	if (type == MUTEX_SPIN) {
		KeInitializeSpinLock((PKSPIN_LOCK)&mp->m_lock);
		mp->mutexused = 2;
	}
	else {
#ifdef USE_MUTEX_EVENT
		KeInitializeEvent((PRKEVENT)&mp->m_lock, SynchronizationEvent, FALSE);
#else
		ExInitializeFastMutex((PFAST_MUTEX)&mp->m_lock);
#endif
		mp->mutexused = 1;
	}
	atomic_inc_64(&zfs_active_mutex);
}

void spl_mutex_destroy(kmutex_t *mp)
{
	if (!mp) return;

	// Make sure any call to KeSetEvent() has completed.
	while (mp->set_event_guard != 0) {
//		DbgBreakPoint();   TODO- understand later why destroy could be called while the mutex is still used.
		kpreempt(KPREEMPT_SYNC);
	}

	mp->initialised = MUTEX_DESTROYED;
	if (mp->m_owner != 0)
		panic("SPL: releasing held mutex");
	atomic_dec_64(&zfs_active_mutex);
}

void spl_mutex_enter(kmutex_t *mp)
{
	kthread_t *thisthread = current_thread();

	VERIFY3P(mp->m_owner, != , 0xdeadbeefdeadbeef);
	if (mp->initialised != MUTEX_INITIALISED)
		panic("%s: mutex not initialised\n", __func__);	
	if (mp->m_owner == thisthread)
		panic("mutex_enter: locking against myself!");

	if (mp->mutexused == 2) {
#ifndef USE_QUEUED_SPINLOCK
		KeAcquireSpinLock((PKSPIN_LOCK)&mp->m_lock,&mp->m_irql);
#else
		KeAcquireInStackQueuedSpinLock((PKSPIN_LOCK)&mp->m_lock,&mp->m_qh);
#endif
		mp->m_owner = thisthread;
	}
	else {
#ifdef USE_MUTEX_EVENT
	again:
		if (InterlockedCompareExchangePointer(&mp->m_owner,
			thisthread, NULL) != NULL) {

			// Failed to CAS-in 'thisthread', as owner was not NULL
			// Wait forever for event to be signaled.
			KeWaitForSingleObject(
				(PRKEVENT)&mp->m_lock,
				Executive,
				KernelMode,
				FALSE,
				NULL
			);

			// We waited, but someone else may have beaten us to it
			// so we need to attempt CAS again
			goto again;
		}
#else
		VERIFY3U(KeGetCurrentIrql(), <= , APC_LEVEL);
		KeEnterCriticalRegion();
		ExAcquireFastMutexUnsafe((PFAST_MUTEX)&mp->m_lock);
		mp->m_owner = thisthread;
		KeLeaveCriticalRegion();
#endif
	}
    
	if (mp->initialised != MUTEX_INITIALISED) {
		panic("%s: race condition with mutex_destroy!\n", __func__);
	}
}

void spl_mutex_exit(kmutex_t* mp)
{
	VERIFY3P(mp->m_owner, != , 0xdeadbeefdeadbeef);
	if (mp->m_owner != current_thread())
		panic("%s: releasing not held/not our lock?\n", __func__);


	if (mp->mutexused == 2) {
		mp->m_owner = NULL;
#ifndef USE_QUEUED_SPINLOCK
		KeReleaseSpinLock((PKSPIN_LOCK)&mp->m_lock, mp->m_irql);
#else
		KeReleaseInStackQueuedSpinLock(&mp->m_qh);
#endif

	}
	else {
		atomic_inc_16(&mp->set_event_guard);
#ifdef USE_MUTEX_EVENT
		// Wake up one waiter now that it is available.
		InterlockedExchangePointer(&mp->m_owner, NULL); // give control back to someone else
		KeSetEvent((PRKEVENT)&mp->m_lock, EVENT_INCREMENT, FALSE);
#else
		VERIFY3U(KeGetCurrentIrql(), <= , APC_LEVEL);
		// prevent tryenter to steal it before a true waiter is woken up (if any)		
		KeEnterCriticalRegion();
		mp->m_owner = NULL;
		ExReleaseFastMutexUnsafe((PFAST_MUTEX)&mp->m_lock);
		KeLeaveCriticalRegion();		
#endif
		atomic_dec_16(&mp->set_event_guard);
	}
}

int spl_mutex_tryenter(kmutex_t *mp)
{
	kthread_t *thisthread = current_thread();

	if (mp->initialised != MUTEX_INITIALISED)
		panic("%s: mutex not initialised\n", __func__);
	if (mp->m_owner == thisthread)
		panic("mutex_tryenter: locking against myself!");
	
	if (mp->mutexused == 2)
		panic("mutex_tryenter: no tryenter on a spinlock!");
	
#ifdef USE_MUTEX_EVENT	
	if (InterlockedCompareExchangePointer(&mp->m_owner, thisthread, NULL) != NULL)
		return 0; // Not held.
	else
		return (1); // held
#else
	KIRQL cIrql = KeGetCurrentIrql();
	if (ExTryToAcquireFastMutex((PFAST_MUTEX)&mp->m_lock)) {
		// Got it.
		mp->m_owner = thisthread;
		// Lower the IRQL to be < APC so special kernel APCs are delivered while holding it. it's safe in ZFS to do so.
		if (cIrql < APC_LEVEL)
			KeLowerIrql(cIrql);
		return (1); // held
	}
	else 
		return(0); // not held			
#endif
}

int spl_mutex_owned(kmutex_t *mp)
{
	return (mp->m_owner == current_thread());
}

struct kthread *spl_mutex_owner(kmutex_t *mp)
{
	return ((struct kthread *)mp->m_owner);
}
