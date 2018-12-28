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
 * Copyright (c) 2017 Jorgen Lundman <lundman@lundman.net>
 */

#include <sys/debug.h>
#include <ntddk.h>
#include <storport.h>  
#include <wmistr.h>
#include <hbapiwmi.h>
#include <wdf.h>
#include <sys/wzvol.h>

extern PDRIVER_OBJECT WIN_DriverObject;

BOOLEAN
wzvol_HwInitialize(__in pHW_HBA_EXT pHBAExt)
{
	UNREFERENCED_PARAMETER(pHBAExt);

	dprintf("%s: entry\n", __func__);
	return TRUE;
}

/**************************************************************************************************/
/*                                                                                                */
/* Create list of LUNs for specified HBA extension.                                               */
/*                                                                                                */
/**************************************************************************************************/
NTSTATUS
wzvol_CreateDeviceList(
	__in       pHW_HBA_EXT    pHBAExt,
	__in       ULONG          NbrLUNs
)
{
	NTSTATUS status = STATUS_SUCCESS;
	ULONG    i,
		len = FIELD_OFFSET(MP_DEVICE_LIST, DeviceInfo) + (NbrLUNs * sizeof(MP_DEVICE_INFO));

	dprintf("%s: entry\n", __func__);

	if (pHBAExt->pDeviceList) {
		ExFreePoolWithTag(pHBAExt->pDeviceList, MP_TAG_GENERAL);
	}

	pHBAExt->pDeviceList = ExAllocatePoolWithTag(NonPagedPool, len, MP_TAG_GENERAL);

	if (!pHBAExt->pDeviceList) {
		status = STATUS_INSUFFICIENT_RESOURCES;
		goto done;
	}

	RtlZeroMemory(pHBAExt->pDeviceList, len);

	pHBAExt->pDeviceList->DeviceCount = NbrLUNs;

	for (i = 0; i < NbrLUNs; i++) {
		pHBAExt->pDeviceList->DeviceInfo[i].LunID = (UCHAR)i;
	}

done:
	return status;
}                                                     // End MpCreateDeviceList().

ULONG
wzvol_HwFindAdapter(
	__in       pHW_HBA_EXT                     pHBAExt,           // Adapter device-object extension from StorPort.
	__in       PVOID                           pHwContext,        // Pointer to context.
	__in       PVOID                           pBusInformation,   // Miniport's FDO.
	__in       PVOID                           pLowerDO,          // Device object beneath FDO.
	__in       PCHAR                           pArgumentString,
	__in __out PPORT_CONFIGURATION_INFORMATION pConfigInfo,
	__in       PBOOLEAN                        pBAgain
)
{
	ULONG i, len, status = SP_RETURN_FOUND;
	PCHAR pChar;

	dprintf("%s: entry\n", __func__);

#if defined(_AMD64_)

	KLOCK_QUEUE_HANDLE LockHandle;

#else

	KIRQL              SaveIrql;

#endif

	UNREFERENCED_PARAMETER(pHwContext);
	UNREFERENCED_PARAMETER(pBusInformation);
	UNREFERENCED_PARAMETER(pLowerDO);
	UNREFERENCED_PARAMETER(pArgumentString);

	dprintf("%s: pHBAExt = 0x%p, pConfigInfo = 0x%p\n", __func__, 
		pHBAExt, pConfigInfo);

	pHBAExt->pwzvolDrvObj = &STOR_wzvolDriverInfo;            // Copy master object from static variable.

	InitializeListHead(&pHBAExt->MPIOLunList);        // Initialize list head.
	InitializeListHead(&pHBAExt->LUList);

	KeInitializeSpinLock(&pHBAExt->WkItemsLock);      // Initialize locks.     
	KeInitializeSpinLock(&pHBAExt->WkRoutinesLock);
	KeInitializeSpinLock(&pHBAExt->MPHBAObjLock);
	KeInitializeSpinLock(&pHBAExt->LUListLock);

	pHBAExt->HostTargetId = (UCHAR)pHBAExt->pwzvolDrvObj->wzvolRegInfo.InitiatorID;

	pHBAExt->pDrvObj = pHBAExt->pwzvolDrvObj->pDriverObj;

	wzvol_CreateDeviceList(pHBAExt, pHBAExt->pwzvolDrvObj->wzvolRegInfo.NbrLUNsperHBA);

	if (!pHBAExt->pPrevDeviceList) {
		pHBAExt->pPrevDeviceList = pHBAExt->pDeviceList;
	}

	pConfigInfo->VirtualDevice = TRUE;                        // Inidicate no real hardware.
	pConfigInfo->WmiDataProvider = TRUE;                        // Indicate WMI provider.
	pConfigInfo->MaximumTransferLength = SP_UNINITIALIZED_VALUE;      // Indicate unlimited.
	pConfigInfo->AlignmentMask = 0x3;                         // Indicate DWORD alignment.
	pConfigInfo->CachesData = FALSE;                       // Indicate miniport wants flush and shutdown notification.
	pConfigInfo->MaximumNumberOfTargets = SCSI_MAXIMUM_TARGETS;        // Indicate maximum targets.
	pConfigInfo->NumberOfBuses = 1;                           // Indicate number of busses.
	pConfigInfo->SynchronizationModel = StorSynchronizeFullDuplex;   // Indicate full-duplex.
	pConfigInfo->ScatterGather = TRUE;                        // Indicate scatter-gather (explicit setting needed for Win2003 at least).

	// Save Vendor Id, Product Id, Revision in device extension.

	pChar = (PCHAR)pHBAExt->pwzvolDrvObj->wzvolRegInfo.VendorId.Buffer;
	len = min(8, (pHBAExt->pwzvolDrvObj->wzvolRegInfo.VendorId.Length / 2));
	for (i = 0; i < len; i++, pChar += 2)
		pHBAExt->VendorId[i] = *pChar;

	pChar = (PCHAR)pHBAExt->pwzvolDrvObj->wzvolRegInfo.ProductId.Buffer;
	len = min(16, (pHBAExt->pwzvolDrvObj->wzvolRegInfo.ProductId.Length / 2));
	for (i = 0; i < len; i++, pChar += 2)
		pHBAExt->ProductId[i] = *pChar;

	pChar = (PCHAR)pHBAExt->pwzvolDrvObj->wzvolRegInfo.ProductRevision.Buffer;
	len = min(4, (pHBAExt->pwzvolDrvObj->wzvolRegInfo.ProductRevision.Length / 2));
	for (i = 0; i < len; i++, pChar += 2)
		pHBAExt->ProductRevision[i] = *pChar;

	pHBAExt->NbrLUNsperHBA = pHBAExt->pwzvolDrvObj->wzvolRegInfo.NbrLUNsperHBA;

	// Add HBA extension to master driver object's linked list.

#if defined(_AMD64_)
	KeAcquireInStackQueuedSpinLock(&pHBAExt->pwzvolDrvObj->DrvInfoLock, &LockHandle);
#else
	KeAcquireSpinLock(&pHBAExt->pwzvolDrvObj->DrvInfoLock, &SaveIrql);
#endif

	InsertTailList(&pHBAExt->pwzvolDrvObj->ListMPHBAObj, &pHBAExt->List);

	pHBAExt->pwzvolDrvObj->DrvInfoNbrMPHBAObj++;

#if defined(_AMD64_)
	KeReleaseInStackQueuedSpinLock(&LockHandle);
#else
	KeReleaseSpinLock(&pHBAExt->pwzvolDrvObj->DrvInfoLock, SaveIrql);
#endif

	InitializeWmiContext(pHBAExt);

	*pBAgain = FALSE;

	return status;
}



#define StorPortMaxWMIEventSize 0x80                  // Maximum WMIEvent size StorPort will support.
#define InstName L"vHBA"

/**************************************************************************************************/
/*                                                                                                */
/**************************************************************************************************/
void
	wzvol_HwReportAdapter(__in pHW_HBA_EXT pHBAExt)
{
	dprintf("%s: entry\n", __func__);

#if 1
	NTSTATUS               status;
	PWNODE_SINGLE_INSTANCE pWnode;
	ULONG                  WnodeSize,
		WnodeSizeInstanceName,
		WnodeSizeDataBlock,
		length,
		size;
	GUID                   lclGuid = MSFC_AdapterEvent_GUID;
	UNICODE_STRING         lclInstanceName;
	UCHAR                  myPortWWN[8] = { 1, 2, 3, 4, 5, 6, 7, 8 };
	PMSFC_AdapterEvent     pAdapterArr;

	// With the instance name used here and with the rounding-up to 4-byte alignment of the data portion used here,
	// 0x34 (52) bytes are available for the actual data of the WMI event.  (The 0x34 bytes result from the fact that
	// StorPort at present (August 2008) allows 0x80 bytes for the entire WMIEvent (header, instance name and data);
	// the header is 0x40 bytes; the instance name used here results in 0xA bytes, and the rounding up consumes 2 bytes;
	// in other words, 0x80 - (0x40 + 0x0A + 0x02)).

	RtlInitUnicodeString(&lclInstanceName, InstName); // Set Unicode descriptor for instance name.

	// A WMIEvent structure consists of header, instance name and data block.

	WnodeSize = sizeof(WNODE_SINGLE_INSTANCE);

	// Because the first field in the data block, EventType, is a ULONG, ensure that the data block begins on a
	// 4-byte boundary (as will be calculated in DataBlockOffset).

	WnodeSizeInstanceName = sizeof(USHORT) +          // Size of USHORT at beginning plus
		lclInstanceName.Length;   //   size of instance name.
	WnodeSizeInstanceName =                           // Round length up to multiple of 4 (if needed).
		(ULONG)WDF_ALIGN_SIZE_UP(WnodeSizeInstanceName, sizeof(ULONG));

	WnodeSizeDataBlock = MSFC_AdapterEvent_SIZE;   // Size of data block.

	size = WnodeSize +                    // Size of WMIEvent.         
		WnodeSizeInstanceName +
		WnodeSizeDataBlock;

	pWnode = ExAllocatePoolWithTag(NonPagedPool, size, MP_TAG_GENERAL);

	if (NULL != pWnode) {                               // Good?
		RtlZeroMemory(pWnode, size);

		// Fill out most of header. StorPort will set the ProviderId and TimeStamp in the header.

		pWnode->WnodeHeader.BufferSize = size;
		pWnode->WnodeHeader.Version = 1;
		RtlCopyMemory(&pWnode->WnodeHeader.Guid, &lclGuid, sizeof(lclGuid));
		pWnode->WnodeHeader.Flags = WNODE_FLAG_EVENT_ITEM |
			WNODE_FLAG_SINGLE_INSTANCE;

		// Say where to find instance name and the data block and what is the data block's size.

		pWnode->OffsetInstanceName = WnodeSize;
		pWnode->DataBlockOffset = WnodeSize + WnodeSizeInstanceName;
		pWnode->SizeDataBlock = WnodeSizeDataBlock;

		// Copy the instance name.

		size -= WnodeSize;                            // Length remaining and available.
		status = WDF_WMI_BUFFER_APPEND_STRING(        // Copy WCHAR string, preceded by its size.
			WDF_PTR_ADD_OFFSET(pWnode, pWnode->OffsetInstanceName),
			size,                                     // Length available for copying.
			&lclInstanceName,                         // Unicode string whose WCHAR buffer is to be copied.
			&length                                   // Variable to receive size needed.
		);

		if (STATUS_SUCCESS != status) {                 // A problem?
			ASSERT(FALSE);
		}

		pAdapterArr =                                 // Point to data block.
			WDF_PTR_ADD_OFFSET(pWnode, pWnode->DataBlockOffset);

		// Copy event code and WWN.

		pAdapterArr->EventType = HBA_EVENT_ADAPTER_ADD;

		RtlCopyMemory(pAdapterArr->PortWWN, myPortWWN, sizeof(myPortWWN));

		// Ask StorPort to announce the event.

		StorPortNotification(WMIEvent,
			pHBAExt,
			pWnode,
			0xFF);                   // Notification pertains to an HBA.

		ExFreePoolWithTag(pWnode, MP_TAG_GENERAL);
	}
	else {
	}
#endif
}

/**************************************************************************************************/
/*                                                                                                */
/**************************************************************************************************/
void
wzvol_HwReportLink(__in pHW_HBA_EXT pHBAExt)
{
	dprintf("%s: entry\n", __func__);

#if 1
	NTSTATUS               status;
	PWNODE_SINGLE_INSTANCE pWnode;
	PMSFC_LinkEvent        pLinkEvent;
	ULONG                  WnodeSize,
		WnodeSizeInstanceName,
		WnodeSizeDataBlock,
		length,
		size;
	GUID                   lclGuid = MSFC_LinkEvent_GUID;
	UNICODE_STRING         lclInstanceName;

#define RLIRBufferArraySize 0x10                  // Define 16 entries in MSFC_LinkEvent.RLIRBuffer[].

	UCHAR                  myAdapterWWN[8] = { 1, 2, 3, 4, 5, 6, 7, 8 },
		myRLIRBuffer[RLIRBufferArraySize] = { 10, 11, 12, 13, 14, 15, 16, 17, 20, 21, 22, 23, 24, 25, 26, 0xFF };

	RtlInitUnicodeString(&lclInstanceName, InstName); // Set Unicode descriptor for instance name.

	WnodeSize = sizeof(WNODE_SINGLE_INSTANCE);
	WnodeSizeInstanceName = sizeof(USHORT) +          // Size of USHORT at beginning plus
		lclInstanceName.Length;   //   size of instance name.
	WnodeSizeInstanceName =                           // Round length up to multiple of 4 (if needed).
		(ULONG)WDF_ALIGN_SIZE_UP(WnodeSizeInstanceName, sizeof(ULONG));
	WnodeSizeDataBlock =                           // Size of data.
		FIELD_OFFSET(MSFC_LinkEvent, RLIRBuffer) +
		sizeof(myRLIRBuffer);

	size = WnodeSize +                    // Size of WMIEvent.         
		WnodeSizeInstanceName +
		WnodeSizeDataBlock;

	pWnode = ExAllocatePoolWithTag(NonPagedPool, size, MP_TAG_GENERAL);

	if (NULL != pWnode) {                               // Good?
		RtlZeroMemory(pWnode, size);

		// Fill out most of header. StorPort will set the ProviderId and TimeStamp in the header.

		pWnode->WnodeHeader.BufferSize = size;
		pWnode->WnodeHeader.Version = 1;
		RtlCopyMemory(&pWnode->WnodeHeader.Guid, &lclGuid, sizeof(lclGuid));
		pWnode->WnodeHeader.Flags = WNODE_FLAG_EVENT_ITEM |
			WNODE_FLAG_SINGLE_INSTANCE;

		// Say where to find instance name and the data block and what is the data block's size.

		pWnode->OffsetInstanceName = WnodeSize;
		pWnode->DataBlockOffset = WnodeSize + WnodeSizeInstanceName;
		pWnode->SizeDataBlock = WnodeSizeDataBlock;

		// Copy the instance name.

		size -= WnodeSize;                            // Length remaining and available.
		status = WDF_WMI_BUFFER_APPEND_STRING(        // Copy WCHAR string, preceded by its size.
			WDF_PTR_ADD_OFFSET(pWnode, pWnode->OffsetInstanceName),
			size,                                     // Length available for copying.
			&lclInstanceName,                         // Unicode string whose WCHAR buffer is to be copied.
			&length                                   // Variable to receive size needed.
		);

		if (STATUS_SUCCESS != status) {                 // A problem?
			ASSERT(FALSE);
		}

		pLinkEvent =                                  // Point to data block.
			WDF_PTR_ADD_OFFSET(pWnode, pWnode->DataBlockOffset);

		// Copy event code, WWN, buffer size and buffer contents.

		pLinkEvent->EventType = HBA_EVENT_LINK_INCIDENT;

		RtlCopyMemory(pLinkEvent->AdapterWWN, myAdapterWWN, sizeof(myAdapterWWN));

		pLinkEvent->RLIRBufferSize = sizeof(myRLIRBuffer);

		RtlCopyMemory(pLinkEvent->RLIRBuffer, myRLIRBuffer, sizeof(myRLIRBuffer));

		StorPortNotification(WMIEvent,
			pHBAExt,
			pWnode,
			0xFF);                   // Notification pertains to an HBA.

		ExFreePoolWithTag(pWnode, MP_TAG_GENERAL);
	}
	else {
	}
#endif
}

/**************************************************************************************************/
/*                                                                                                */
/**************************************************************************************************/
void
wzvol_HwReportLog(__in pHW_HBA_EXT pHBAExt)
{
	dprintf("%s: entry\n", __func__);

#if 1
	NTSTATUS               status;
	PWNODE_SINGLE_INSTANCE pWnode;
	ULONG                  WnodeSize,
		WnodeSizeInstanceName,
		WnodeSizeDataBlock,
		length,
		size;
	UNICODE_STRING         lclInstanceName;
	PIO_ERROR_LOG_PACKET   pLogError;

	RtlInitUnicodeString(&lclInstanceName, InstName); // Set Unicode descriptor for instance name.

	WnodeSize = sizeof(WNODE_SINGLE_INSTANCE);
	WnodeSizeInstanceName = sizeof(USHORT) +          // Size of USHORT at beginning plus
		lclInstanceName.Length;   //   size of instance name.
	WnodeSizeInstanceName =                           // Round length up to multiple of 4 (if needed).
		(ULONG)WDF_ALIGN_SIZE_UP(WnodeSizeInstanceName, sizeof(ULONG));
	WnodeSizeDataBlock = sizeof(IO_ERROR_LOG_PACKET);       // Size of data.

	size = WnodeSize +                    // Size of WMIEvent.         
		WnodeSizeInstanceName +
		WnodeSizeDataBlock;

	pWnode = ExAllocatePoolWithTag(NonPagedPool, size, MP_TAG_GENERAL);

	if (NULL != pWnode) {                               // Good?
		RtlZeroMemory(pWnode, size);

		// Fill out most of header. StorPort will set the ProviderId and TimeStamp in the header.

		pWnode->WnodeHeader.BufferSize = size;
		pWnode->WnodeHeader.Version = 1;
		pWnode->WnodeHeader.Flags = WNODE_FLAG_EVENT_ITEM |
			WNODE_FLAG_LOG_WNODE;

		pWnode->WnodeHeader.HistoricalContext = 9;

		// Say where to find instance name and the data block and what is the data block's size.

		pWnode->OffsetInstanceName = WnodeSize;
		pWnode->DataBlockOffset = WnodeSize + WnodeSizeInstanceName;
		pWnode->SizeDataBlock = WnodeSizeDataBlock;

		// Copy the instance name.

		size -= WnodeSize;                            // Length remaining and available.
		status = WDF_WMI_BUFFER_APPEND_STRING(        // Copy WCHAR string, preceded by its size.
			WDF_PTR_ADD_OFFSET(pWnode, pWnode->OffsetInstanceName),
			size,                                     // Length available for copying.
			&lclInstanceName,                         // Unicode string whose WCHAR buffer is to be copied.
			&length                                   // Variable to receive size needed.
		);

		if (STATUS_SUCCESS != status) {                 // A problem?
			ASSERT(FALSE);
		}

		pLogError =                                    // Point to data block.
			WDF_PTR_ADD_OFFSET(pWnode, pWnode->DataBlockOffset);

		pLogError->UniqueErrorValue = 0x40;
		pLogError->FinalStatus = 0x41;
		pLogError->ErrorCode = 0x42;

		StorPortNotification(WMIEvent,
			pHBAExt,
			pWnode,
			0xFF);                   // Notification pertains to an HBA.

		ExFreePoolWithTag(pWnode, MP_TAG_GENERAL);
	}
	else {
	}
#endif
}                                                     // End MpHwReportLog().

/**************************************************************************************************/
/*                                                                                                */
/**************************************************************************************************/
BOOLEAN
wzvol_HwResetBus(
		__in pHW_HBA_EXT          pHBAExt,       // Adapter device-object extension from StorPort.
		__in ULONG                BusId
	)
{
	UNREFERENCED_PARAMETER(pHBAExt);
	UNREFERENCED_PARAMETER(BusId);

	// To do: At some future point, it may be worthwhile to ensure that any SRBs being handled be completed at once.
	//        Practically speaking, however, it seems that the only SRBs that would not be completed very quickly
	//        would be those handled by the worker thread. In the future, therefore, there might be a global flag
	//        set here to instruct the thread to complete outstanding I/Os as they appear; but a period for that
	//        happening would have to be devised (such completion shouldn't be unbounded).

	return TRUE;
}                                                     // End MpHwResetBus().

/**************************************************************************************************/
/*                                                                                                */
/**************************************************************************************************/
NTSTATUS
wzvol_HandleRemoveDevice(
		__in pHW_HBA_EXT             pHBAExt,// Adapter device-object extension from StorPort.
		__in PSCSI_PNP_REQUEST_BLOCK pSrb
	)
{
	UNREFERENCED_PARAMETER(pHBAExt);

	pSrb->SrbStatus = SRB_STATUS_BAD_FUNCTION;

	return STATUS_UNSUCCESSFUL;
}                                                     // End MpHandleRemoveDevice().

/**************************************************************************************************/
/*                                                                                                */
/**************************************************************************************************/
NTSTATUS
wzvol_HandleQueryCapabilities(
		__in pHW_HBA_EXT             pHBAExt,// Adapter device-object extension from StorPort.
		__in PSCSI_PNP_REQUEST_BLOCK pSrb
	)
{
	NTSTATUS                  status = STATUS_SUCCESS;
	PSTOR_DEVICE_CAPABILITIES pStorageCapabilities = (PSTOR_DEVICE_CAPABILITIES)pSrb->DataBuffer;

	UNREFERENCED_PARAMETER(pHBAExt);

	dprintf("%s: entry\n", __func__);

	RtlZeroMemory(pStorageCapabilities, pSrb->DataTransferLength);

	pStorageCapabilities->Removable = FALSE;
	pStorageCapabilities->SurpriseRemovalOK = FALSE;

	pSrb->SrbStatus = SRB_STATUS_SUCCESS;

	return status;
}                                                     // End MpHandleQueryCapabilities().

/**************************************************************************************************/
/*                                                                                                */
/**************************************************************************************************/
NTSTATUS
wzvol_HwHandlePnP(
		__in pHW_HBA_EXT              pHBAExt,  // Adapter device-object extension from StorPort.
		__in PSCSI_PNP_REQUEST_BLOCK  pSrb
	)
{
	NTSTATUS status = STATUS_SUCCESS;
	dprintf("%s: entry\n", __func__);

#if 1
	switch (pSrb->PnPAction) {

	case StorRemoveDevice:
		status = wzvol_HandleRemoveDevice(pHBAExt, pSrb);

		break;

	case StorQueryCapabilities:
		status = wzvol_HandleQueryCapabilities(pHBAExt, pSrb);

		break;

	default:
		pSrb->SrbStatus = SRB_STATUS_SUCCESS;         // Do nothing.
	}

	if (STATUS_SUCCESS != status) {
	}
#endif
	return status;
}

/**************************************************************************************************/
/*                                                                                                */
/**************************************************************************************************/
BOOLEAN
wzvol_HwStartIo(
		__in       pHW_HBA_EXT          pHBAExt,  // Adapter device-object extension from StorPort.
		__in __out PSCSI_REQUEST_BLOCK  pSrb
	)
{
	dprintf("%s: entry\n", __func__);

#if 1
	UCHAR                     srbStatus = SRB_STATUS_INVALID_REQUEST;
	BOOLEAN                   bFlag;
	NTSTATUS                  status;
	UCHAR                     Result = ResultDone;

	dprintf(
		"MpHwStartIo:  SCSI Request Block = %!SRB!\n",
		pSrb);

	_InterlockedExchangeAdd((volatile LONG *)&pHBAExt->SRBsSeen, 1);   // Bump count of SRBs encountered.

	// Next, if true, will cause StorPort to remove the associated LUNs if, for example, devmgmt.msc is asked "scan for hardware changes."

	if (pHBAExt->bDontReport) {                       // Act as though the HBA/path is gone?
		srbStatus = SRB_STATUS_INVALID_LUN;
		goto done;
	}

	switch (pSrb->Function) {

	case SRB_FUNCTION_EXECUTE_SCSI:
		srbStatus = ScsiExecuteMain(pHBAExt, pSrb, &Result);
		break;

	case SRB_FUNCTION_WMI:
		_InterlockedExchangeAdd((volatile LONG *)&pHBAExt->WMISRBsSeen, 1);
		bFlag = HandleWmiSrb(pHBAExt, (PSCSI_WMI_REQUEST_BLOCK)pSrb);
		srbStatus = TRUE == bFlag ? SRB_STATUS_SUCCESS : SRB_STATUS_INVALID_REQUEST;
		break;

	case SRB_FUNCTION_RESET_LOGICAL_UNIT:
		StorPortCompleteRequest(
			pHBAExt,
			pSrb->PathId,
			pSrb->TargetId,
			pSrb->Lun,
			SRB_STATUS_BUSY
		);
		srbStatus = SRB_STATUS_SUCCESS;
		break;

	case SRB_FUNCTION_RESET_DEVICE:
		StorPortCompleteRequest(
			pHBAExt,
			pSrb->PathId,
			pSrb->TargetId,
			SP_UNTAGGED,
			SRB_STATUS_TIMEOUT
		);
		srbStatus = SRB_STATUS_SUCCESS;
		break;

	case SRB_FUNCTION_PNP:
		status = wzvol_HwHandlePnP(pHBAExt, (PSCSI_PNP_REQUEST_BLOCK)pSrb);
		srbStatus = pSrb->SrbStatus;

		break;

	case SRB_FUNCTION_POWER:
		// Do nothing.
		srbStatus = SRB_STATUS_SUCCESS;

		break;

	case SRB_FUNCTION_SHUTDOWN:
		// Do nothing.
		srbStatus = SRB_STATUS_SUCCESS;

		break;

	default:
		dprintf("MpHwStartIo: Unknown Srb Function = 0x%x\n", pSrb->Function);
		srbStatus = SRB_STATUS_INVALID_REQUEST;
		break;

	} // switch (pSrb->Function)

done:
	if (ResultDone == Result) {                         // Complete now?
		pSrb->SrbStatus = srbStatus;

		// Note:  A miniport with real hardware would not always be calling RequestComplete from HwStorStartIo.  Rather,
		//        the miniport would typically be doing real I/O and would call RequestComplete only at the end of that
		//        real I/O, in its HwStorInterrupt or in a DPC routine.

		StorPortNotification(RequestComplete, pHBAExt, pSrb);
	}

	dprintf("MpHwStartIo - OUT\n");
#endif
	return TRUE;
}

/**************************************************************************************************/
/*                                                                                                */
/**************************************************************************************************/
SCSI_ADAPTER_CONTROL_STATUS
wzvol_HwAdapterControl(
		__in pHW_HBA_EXT               pHBAExt, // Adapter device-object extension from StorPort.
		__in SCSI_ADAPTER_CONTROL_TYPE ControlType,
		__in PVOID                     pParameters
	)
{
	PSCSI_SUPPORTED_CONTROL_TYPE_LIST pCtlTypList;
	ULONG                             i;

	dprintf("MpHwAdapterControl:  ControlType = %d\n", ControlType);

	pHBAExt->AdapterState = ControlType;

	switch (ControlType) {
	case ScsiQuerySupportedControlTypes:
		dprintf("MpHwAdapterControl: ScsiQuerySupportedControlTypes\n");

		// Ggt pointer to control type list
		pCtlTypList = (PSCSI_SUPPORTED_CONTROL_TYPE_LIST)pParameters;

		// Cycle through list to set TRUE for each type supported
		// making sure not to go past the MaxControlType
		for (i = 0; i < pCtlTypList->MaxControlType; i++)
			if (i == ScsiQuerySupportedControlTypes ||
				i == ScsiStopAdapter || i == ScsiRestartAdapter ||
				i == ScsiSetBootConfig || i == ScsiSetRunningConfig)
			{
				pCtlTypList->SupportedTypeList[i] = TRUE;
			}
		break;

	case ScsiStopAdapter:
		dprintf("MpHwAdapterControl:  ScsiStopAdapter\n");

		// Free memory allocated for disk
		wzvol_StopAdapter(pHBAExt);

		break;

	case ScsiRestartAdapter:
		dprintf("MpHwAdapterControl:  ScsiRestartAdapter\n");

		/* To Do: Add some function. */

		break;

	case ScsiSetBootConfig:
		dprintf("MpHwAdapterControl:  ScsiSetBootConfig\n");

		break;

	case ScsiSetRunningConfig:
		dprintf("MpHwAdapterControl:  ScsiSetRunningConfig\n");

		break;

	default:
		dprintf("MpHwAdapterControl:  UNKNOWN\n");

		break;
	}

	dprintf("MpHwAdapterControl - OUT\n");

	return ScsiAdapterControlSuccess;
}

/**************************************************************************************************/
/*                                                                                                */
/**************************************************************************************************/
VOID
wzvol_StopAdapter(__in pHW_HBA_EXT pHBAExt)               // Adapter device-object extension from StorPort.
{
	pHW_LU_EXTENSION      pLUExt,
		pLUExt2;
	PLIST_ENTRY           pNextEntry,
		pNextEntry2;
	pwzvolDriverInfo         pwzvolDrvInfo = pHBAExt->pwzvolDrvObj;
	pHW_LU_EXTENSION_MPIO pLUMPIOExt = NULL;          // Prevent C4701 warning.

#if defined(_AMD64_)
	KLOCK_QUEUE_HANDLE    LockHandle;
#else
	KIRQL                 SaveIrql;
#endif

	dprintf("%s: entry\n", __func__);

	// Clean up the "disk buffers."

	for (                                             // Go through linked list of LUN extensions for this HBA.
		pNextEntry = pHBAExt->LUList.Flink;
		pNextEntry != &pHBAExt->LUList;
		pNextEntry = pNextEntry->Flink
		) {
		pLUExt = CONTAINING_RECORD(pNextEntry, HW_LU_EXTENSION, List);

		if (pwzvolDrvInfo->wzvolRegInfo.bCombineVirtDisks) {// MPIO support?
			pLUMPIOExt = pLUExt->pLUMPIOExt;

			if (!pLUMPIOExt) {                        // No MPIO extension?
				break;
			}

#if defined(_AMD64_)
			KeAcquireInStackQueuedSpinLock(&pLUMPIOExt->LUExtMPIOLock, &LockHandle);
#else
			KeAcquireSpinLock(&pLUMPIOExt->LUExtMPIOLock, &SaveIrql);
#endif

			for (                                     // Go through linked list of LUN extensions for the MPIO collector object (HW_LU_EXTENSION_MPIO).
				pNextEntry2 = pLUMPIOExt->LUExtList.Flink;
				pNextEntry2 != &pLUMPIOExt->LUExtList;
				pNextEntry2 = pNextEntry2->Flink
				) {
				pLUExt2 = CONTAINING_RECORD(pNextEntry2, HW_LU_EXTENSION, MPIOList);

				if (pLUExt2 == pLUExt) {                // Pointing to same LUN extension?
					break;
				}
			}

			if (pNextEntry2 != &pLUMPIOExt->LUExtList) {// Found it?
				RemoveEntryList(pNextEntry2);         // Remove LU extension from MPIO collector object.    

				pLUMPIOExt->NbrRealLUNs--;

				if (0 == pLUMPIOExt->NbrRealLUNs) {     // Was this the last LUN extension in the MPIO collector object?
					ExFreePool(pLUExt->pDiskBuf);
				}
			}

#if defined(_AMD64_)
			KeReleaseInStackQueuedSpinLock(&LockHandle);
#else
			KeReleaseSpinLock(&pLUMPIOExt->LUExtMPIOLock, SaveIrql);
#endif
		}
		else {
			ExFreePool(pLUExt->pDiskBuf);
		}
	}

	// Clean up the linked list of MPIO collector objects, if needed.

	if (pwzvolDrvInfo->wzvolRegInfo.bCombineVirtDisks) {    // MPIO support?
#if defined(_AMD64_)
		KeAcquireInStackQueuedSpinLock(               // Serialize the linked list of MPIO collector objects.
			&pwzvolDrvInfo->MPIOExtLock, &LockHandle);
#else
		KeAcquireSpinLock(&pMPDrvInfo->MPIOExtLock, &SaveIrql);
#endif

		for (                                         // Go through linked list of MPIO collector objects for this miniport driver.
			pNextEntry = pwzvolDrvInfo->ListMPIOExt.Flink;
			pNextEntry != &pwzvolDrvInfo->ListMPIOExt;
			pNextEntry = pNextEntry2
			) {
			pLUMPIOExt = CONTAINING_RECORD(pNextEntry, HW_LU_EXTENSION_MPIO, List);

			if (!pLUMPIOExt) {                        // No MPIO extension?
				break;
			}

			pNextEntry2 = pNextEntry->Flink;          // Save forward pointer in case MPIO collector object containing forward pointer is freed.

			if (0 == pLUMPIOExt->NbrRealLUNs) {         // No real LUNs (HW_LU_EXTENSION) left?
				RemoveEntryList(pNextEntry);          // Remove MPIO collector object from miniport driver object.    

				ExFreePoolWithTag(pLUMPIOExt, MP_TAG_GENERAL);
			}
		}

#if defined(_AMD64_)
		KeReleaseInStackQueuedSpinLock(&LockHandle);
#else
		KeReleaseSpinLock(&pMPDrvInfo->MPIOExtLock, SaveIrql);
#endif
	}

	//done:
	return;
}

/**************************************************************************************************/
/*                                                                                                */
/* Return device type for specified device on specified HBA extension.                            */
/*                                                                                                */
/**************************************************************************************************/
UCHAR
wzvol_GetDeviceType(
		__in pHW_HBA_EXT          pHBAExt,    // Adapter device-object extension from StorPort.
		__in UCHAR                PathId,
		__in UCHAR                TargetId,
		__in UCHAR                Lun
	)
{
	pMP_DEVICE_LIST pDevList = pHBAExt->pDeviceList;
	ULONG           i;
	UCHAR           type = DEVICE_NOT_FOUND;

	UNREFERENCED_PARAMETER(PathId);

	dprintf("%s: entry\n", __func__);

	if (!pDevList || 0 == pDevList->DeviceCount) {
		goto done;
	}

	for (i = 0; i < pDevList->DeviceCount; i++) {    // Find the matching LUN (if any).
		if (
			TargetId == pDevList->DeviceInfo[i].TargetID
			&&
			Lun == pDevList->DeviceInfo[i].LunID
			) {
			type = pDevList->DeviceInfo[i].DeviceType;
			goto done;
		}
	}

done:
	return type;
}

/**************************************************************************************************/
/*                                                                                                */
/* MPTracingInit.                                                                                 */
/*                                                                                                */
/**************************************************************************************************/
VOID
wzvol_TracingInit(
		__in PVOID pArg1,
		__in PVOID pArg2
	)
{
//	WPP_INIT_TRACING(pArg1, pArg2);
}                                                     // End MPTracingInit().

/**************************************************************************************************/
/*                                                                                                */
/* MPTracingCleanUp.                                                                              */
/*                                                                                                */
/* This is called when the driver is being unloaded.                                              */
/*                                                                                                */
/**************************************************************************************************/
VOID
wzvol_TracingCleanup(__in PVOID pArg1)
{
#if 1
	dprintf("MPTracingCleanUp entered\n");

	//WPP_CLEANUP(pArg1);
#endif
}

/**************************************************************************************************/
/*                                                                                                */
/* MpHwFreeAdapterResources.                                                                      */
/*                                                                                                */
/**************************************************************************************************/
VOID
wzvol_HwFreeAdapterResources(__in pHW_HBA_EXT pHBAExt)
{
#if 1
	PLIST_ENTRY           pNextEntry;
	pHW_HBA_EXT           pLclHBAExt;
#if defined(_AMD64_)
	KLOCK_QUEUE_HANDLE    LockHandle;
#else
	KIRQL                 SaveIrql;
#endif

	dprintf("MpHwFreeAdapterResources entered, pHBAExt = 0x%p\n", pHBAExt);

#if defined(_AMD64_)
	KeAcquireInStackQueuedSpinLock(&pHBAExt->pwzvolDrvObj->DrvInfoLock, &LockHandle);
#else
	KeAcquireSpinLock(&pHBAExt->pwzvolDrvObj->DrvInfoLock, &SaveIrql);
#endif

	for (                                             // Go through linked list of HBA extensions.
		pNextEntry = pHBAExt->pwzvolDrvObj->ListMPHBAObj.Flink;
		pNextEntry != &pHBAExt->pwzvolDrvObj->ListMPHBAObj;
		pNextEntry = pNextEntry->Flink
		) {
		pLclHBAExt = CONTAINING_RECORD(pNextEntry, HW_HBA_EXT, List);

		if (pLclHBAExt == pHBAExt) {                    // Is this entry the same as pHBAExt?
			RemoveEntryList(pNextEntry);
			pHBAExt->pwzvolDrvObj->DrvInfoNbrMPHBAObj--;
			break;
		}
	}

#if defined(_AMD64_)
	KeReleaseInStackQueuedSpinLock(&LockHandle);
#else
	KeReleaseSpinLock(&pHBAExt->pwzvolDrvObj->DrvInfoLock, SaveIrql);
#endif

	ExFreePoolWithTag(pHBAExt->pDeviceList, MP_TAG_GENERAL);
#endif
}

/**************************************************************************************************/
/*                                                                                                */
/* MpCompleteIrp.                                                                                 */
/*                                                                                                */
/**************************************************************************************************/
VOID
wzvol_CompleteIrp(
		__in pHW_HBA_EXT   pHBAExt,             // Adapter device-object extension from StorPort.
		__in PIRP          pIrp
	)
{

#define            minLen 16
#if 1
	dprintf("MpCompleteIrp entered\n");

	if (NULL != pIrp) {
		NTSTATUS           Status = STATUS_SUCCESS;
		PIO_STACK_LOCATION pIrpStack = IoGetCurrentIrpStackLocation(pIrp);
		ULONG              inputBufferLength;

		inputBufferLength =
			pIrpStack->Parameters.DeviceIoControl.InputBufferLength;

		switch (pIrpStack->Parameters.DeviceIoControl.IoControlCode) {
		case IOCTL_MINIPORT_PROCESS_SERVICE_IRP:

			if (inputBufferLength < minLen) {

				Status = STATUS_BUFFER_TOO_SMALL;
			}

			break;

		default:
			Status = STATUS_INVALID_DEVICE_REQUEST;

			break;
		}

		RtlMoveMemory(pIrp->AssociatedIrp.SystemBuffer, "123412341234123", minLen);

		pIrp->IoStatus.Status = Status;
		pIrp->IoStatus.Information = 16;

		StorPortCompleteServiceIrp(pHBAExt, pIrp);
	}
#endif
}     

/**************************************************************************************************/
/*                                                                                                */
/* MpQueueServiceIrp.                                                                             */
/*                                                                                                */
/* If there is already an IRP queued, it will be dequeued (and then completed) to make way for    */
/* the IRP supplied here.                                                                         */
/*                                                                                                */
/**************************************************************************************************/
VOID
wzvol_QueueServiceIrp(
		__in pHW_HBA_EXT          pHBAExt,  // Adapter device-object extension from StorPort.
		__in PIRP                 pIrp      // IRP pointer to be queued.
	)
{

#if 1
	PIRP pOldIrp;

	dprintf("MpQueueServiceIrp entered\n");

	pOldIrp = InterlockedExchangePointer(&pHBAExt->pReverseCallIrp, pIrp);

	if (NULL != pOldIrp) {                              // Found an IRP already queued?
		wzvol_CompleteIrp(pHBAExt, pIrp);                   // Complete it.
	}
#endif
}                                                     // End MpQueueServiceIrp().

/**************************************************************************************************/
/*                                                                                                */
/* MpProcServReq.                                                                                 */
/*                                                                                                */
/**************************************************************************************************/
VOID
wzvol_ProcServReq(
		__in pHW_HBA_EXT          pHBAExt,      // Adapter device-object extension from StorPort.
		__in PIRP                 pIrp          // IRP pointer received.
	)
{

#if 1
	dprintf("MpProcServReq entered\n");

	wzvol_QueueServiceIrp(pHBAExt, pIrp);
#endif
}                                                     // End MpProcServReq().

/**************************************************************************************************/
/*                                                                                                */
/* MpCompServReq.                                                                                 */
/*                                                                                                */
/**************************************************************************************************/
VOID
wzvol_CompServReq(__in pHW_HBA_EXT          pHBAExt)      // Adapter device-object extension from StorPort.
{

#if 1
	dprintf("MpHwCompServReq entered\n");

	wzvol_QueueServiceIrp(pHBAExt, NULL);
#endif
}                                                     // End MpCompServReq().
