use AtomicStat
drop function mtb.ufnMTBCustAcctSummaryStatsDelta_HashId
drop function mtb.ufnMTBCustAcctSummaryStatsDelta_TranslateRowToCell
drop function mtb.ufnMTBCustAcctSummaryStatsDelta_TranslateRowToCell_alt
drop function mtb.ufnMTBCustFreqStatsDelta_HashId
drop function mtb.ufnMTBCustFreqStatsDelta_TranslateRowToCell
drop function mtb.ufnMTBCustTrxnStatsDelta_HashId
drop function mtb.ufnMTBCustTrxnStatsDelta_TranslateRowToCell
drop function mtb.ufnMTBDailyCustNegBalDays30and90Delta_HashId
drop function mtb.ufnMTBDailyCustNegBalDays30and90Delta_TranslateRowToCell
drop function mtb.ufnMTBKCPFreqStatsDelta_HashId
drop function mtb.ufnMTBKCPFreqStatsDelta_TranslateRowToCell
drop function mtb.ufnMTBKCPTrxnStatsDelta_HashId
drop function mtb.ufnMTBKCPTrxnStatsDelta_TranslateRowToCell
drop function mtb.ufnMTBPayerGradeNegDelta_HashId
drop function mtb.ufnMTBPayerGradeNegDelta_TranslateRowToCell
drop function mtb.ufnMTBPayerTrxnStatsDelta_HashId
drop function mtb.ufnMTBPayerTrxnStatsDelta_HashId_slim
drop function mtb.ufnMTBPayerTrxnStatsDelta_TranslateRowToCell
drop function mtb.ufnMTBWeeklyBFCustAUStatsDelta_HashId
drop function mtb.ufnMTBWeeklyBFCustAUStatsDelta_TranslateRowToCell
drop function pnc.ufnPncCustomerAccountLevelStat
drop function pnc.ufnPncCustomerAccountLevelStatId
drop function pnc.ufnPncCustomerChannelLocationStat
drop function pnc.ufnPncCustomerChannelLocationStatId
drop function pnc.ufnPncCustomerChannelStat
drop function pnc.ufnPncCustomerChannelStatId
drop function pnc.ufnPncCustomerStat
drop function pnc.ufnPncCustomerStatId
drop function pnc.ufnPncDollarStratGeo
drop function pnc.ufnPncDollarStratGeoId
drop function pnc.ufnPncDollarStratStat
drop function pnc.ufnPncKCPStat
drop function pnc.ufnPncKCPStatId
drop function pnc.ufnPncOnUsAccount
drop function pnc.ufnPncOnUsRouting
drop function pnc.ufnPncPayerRtnAcctNumLenVol
drop function pnc.ufnPncPayerRtnVolume
drop function pnc.ufnPncPayerStat
drop function pnc.ufnPncPayerStatId
drop function pnc.ufnPncTransactionLocationStat
drop function pnc.ufnPncTransactionLocationStatId

drop proc stat.uspFinancialKCPClearedCheckNumberDHayesReview
drop proc mtb.uspMTBBatchDataSetRefreshLogTransferComplete
drop proc mtb.uspMTBBatchDataSetRefreshLogTransferInitiate
drop proc mtb.uspMTBBatchTransferComplete
drop proc mtb.uspMTBCustAcctSummaryStatsDelta_TransferToAtomicStat
drop proc mtb.uspMTBCustFreqStatsDelta_TransferToAtomicStat
drop proc mtb.uspMTBCustTrxnStatsDelta_TransferToAtomicStat
drop proc mtb.uspMTBDailyCustNegBalDays30and90Delta_TransferToAtomicStat
drop proc mtb.uspMTBKCPFreqStatsDelta_TransferToAtomicStat
drop proc mtb.uspMTBKCPTrxnStatsDelta_TransferToAtomicStat
drop proc mtb.uspMTBPayerGradeNegDelta_TransferToAtomicStat
drop proc mtb.uspMTBPayerTrxnStatsDelta_TransferToAtomicStat
drop proc mtb.uspMTBWeeklyBFCustAUStatsDelta_TransferToAtomicStat
drop proc pnc.uspPNCABAAcctLengthStatsDelta_TransferToAtomicStat
drop proc pnc.uspPNCABAStatsDelta_TransferToAtomicStat
drop proc pnc.uspPNCDollarStratStatsDelta_TransferToAtomicStat

drop view mtb.vwMTBBatchStatBatchLogXref
drop view precalc.vwStat_FIPNC_KCP
drop view precalc.vwStat_FIPNC_KCPMaxCheckNumberCleared
