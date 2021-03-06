/* HIST REL PARTY AGREEMENT TRAC*/
/* TRUNCATION OF THE WORK TABLE */
TRUNCATE TABLE EDW_WORK.TRAC_REL_PARTY_AGREEMENT;

/* INSERT AND UPDATE NEW RECORDS OF MASTER CONTRACT*/
INSERT INTO EDW_WORK.TRAC_REL_PARTY_AGREEMENT (DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, DIM_PARTY_NATURAL_KEY_HASH_UUID, REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID, REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,DIM_PRODUCT_NATURAL_KEY_HASH_UUID,DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
	BEGIN_DT, BEGIN_DTM, ROW_PROCESS_DTM, AUDIT_ID, LOGICAL_DELETE_IND, CHECK_SUM, CURRENT_ROW_IND, END_DT, END_DTM, SOURCE_SYSTEM_ID, RESTRICTED_ROW_IND)
SELECT DRVD.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, DRVD.DIM_PARTY_NATURAL_KEY_HASH_UUID, DRVD.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID, DRVD.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID, DRVD.DIM_PRODUCT_NATURAL_KEY_HASH_UUID,
NULL AS DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
DRVD.BEGIN_DT, DRVD.BEGIN_DTM,
CURRENT_TIMESTAMP AS ROW_PROCESS_DTM,
DRVD.AUDIT_ID, DRVD.LOGICAL_DELETE_IND, DRVD.CHECK_SUM,
CASE WHEN ROW_NUMBER() OVER (PARTITION BY DRVD.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID ORDER BY DRVD.END_DTM DESC, DRVD.END_DT DESC) = 1 AND DRVD.END_DT = '9999-12-31' THEN 'T' ELSE 'F' END::BOOLEAN AS CURRENT_ROW_IND,
DRVD.END_DT, DRVD.END_DTM, DRVD.SOURCE_SYSTEM_ID, DRVD.RESTRICTED_ROW_IND
FROM (SELECT UUID_GEN(PREHASH_VALUE(AGREEMENT_SOURCE_CDE, AGREEMENT_TYPE_CDE, AGREEMENT_NR_PFX, AGREEMENT_NR, AGREEMENT_NR_SFX))::UUID AS DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
		, UUID_GEN(PREHASH_VALUE(PARTY_ID))::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID
		, UUID_GEN(PREHASH_VALUE(PARTY_ROLE_CDE))::UUID AS REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID
		, CLEAN_STRING('') AS DIM_PRODUCT_NATURAL_KEY_HASH_UUID
		, CLEAN_STRING('') AS DIM_ACCOUNT_NATURAL_KEY_HASH_UUID
		, CLEAN_STRING('') AS REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID
		, HASH_GEN(PREHASH_VALUE(CLEAN_STRING('') ,CLEAN_STRING('') ,CLEAN_STRING('')))::UUID AS CHECK_SUM
		, ADT.AUDIT_ID AS AUDIT_ID
		, '203' AS SOURCE_SYSTEM_ID
		, 'F'::BOOLEAN AS RESTRICTED_ROW_IND
		, 'F'::BOOLEAN AS LOGICAL_DELETE_IND
		, T1.RCRD_EFFCTV_DT AS BEGIN_DTM
		, T1.RCRD_EFFCTV_DT::date AS BEGIN_DT
		, CURRENT_TIMESTAMP AS ROW_PROCESS_DTM
		, T1.RCRD_TRM_DT::date AS END_DT
		, T1.RCRD_TRM_DT AS END_DTM
		, 'T'::BOOLEAN AS CURRENT_ROW_IND
	FROM (SELECT DISTINCT CLEAN_STRING('') AS AGREEMENT_NR_PFX
			, PREHASH_VALUE(CLEAN_STRING(ICU_ID),CLEAN_STRING(TO_CHAR(SPNSR_CMPNY_ID::INTEGER))) AS AGREEMENT_NR
			, CLEAN_STRING('') AS AGREEMENT_NR_SFX
			, CLEAN_STRING('TRAC') AS AGREEMENT_SOURCE_CDE
			, CLEAN_STRING('MCA') AS AGREEMENT_TYPE_CDE
			, PREHASH_VALUE(CLEAN_STRING('TRAC_ENTITY'),CLEAN_STRING(ICU_ID),CLEAN_STRING(TO_CHAR(SPNSR_CMPNY_ID::INTEGER))) AS PARTY_ID
			, CLEAN_STRING('GRP') AS PARTY_ROLE_CDE
			, COALESCE(TRAC_HIST.RCRD_TRM_DT,'9999-12-31'::timestamp) AS RCRD_TRM_DT
			, COALESCE(TRAC_HIST.RCRD_EFFCTV_DT,'9999-12-31'::timestamp) AS RCRD_EFFCTV_DT

		FROM TRAC_RPGW.DIM_SPONSOR_COMPANY TRC

		LEFT JOIN TRAC_RPGW.DIM_CONSENT_CUST TRAC_HIST
		ON TRC.SPNSR_CMPNY_KEY = TRAC_HIST.CONSENT_CUST_KEY
		AND TRC.SPNSR_CMPNY_ID = TRAC_HIST.CLIENT_ID
		AND TRAC_HIST.RCRD_TRM_DT = '9999-12-31' 
		WHERE UPPER(TRC.ACTV_RCRD_IND) = 'Y') T1

	LEFT JOIN EDW_AUDIT.ETL_BATCH_AUDIT_VW ADT
	ON ADT.RECORD_ALIGNER = 1
	AND ADT.BATCH_NAME = 'Trac_to_CIP') DRVD

LEFT JOIN (SELECT DIM_AGREEMENT_NATURAL_KEY_HASH_UUID::UUID AS DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, DIM_PARTY_NATURAL_KEY_HASH_UUID::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID, REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID::UUID AS REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID, END_DT, CURRENT_ROW_IND, CHECK_SUM::UUID AS CHECK_SUM, SOURCE_SYSTEM_ID
	FROM EDW_WORK.TRAC_REL_PARTY_AGREEMENT) TGT
ON TGT.CURRENT_ROW_IND= TRUE
AND TGT.END_DT='9999-12-31'
AND TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID = DRVD.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
AND TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID = DRVD.DIM_PARTY_NATURAL_KEY_HASH_UUID
AND TGT.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID = DRVD.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID

WHERE CASE WHEN TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID IS NULL THEN 'I'
	WHEN TGT.CHECK_SUM <> DRVD.CHECK_SUM THEN 'U'
	ELSE 'D' END IN ('I','U');

/* INSERT AND UPDATE NEW RECORDS OF MASTER CONTRACT PLAN SPONSOR*/
INSERT INTO EDW_WORK.TRAC_REL_PARTY_AGREEMENT (DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, DIM_PARTY_NATURAL_KEY_HASH_UUID, REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID, REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,DIM_PRODUCT_NATURAL_KEY_HASH_UUID,DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
	BEGIN_DT, BEGIN_DTM, ROW_PROCESS_DTM, AUDIT_ID, LOGICAL_DELETE_IND, CHECK_SUM, CURRENT_ROW_IND, END_DT, END_DTM, SOURCE_SYSTEM_ID, RESTRICTED_ROW_IND)
SELECT DRVD.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, DRVD.DIM_PARTY_NATURAL_KEY_HASH_UUID, DRVD.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID, DRVD.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID, DRVD.DIM_PRODUCT_NATURAL_KEY_HASH_UUID,
NULL AS DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
DRVD.BEGIN_DT, DRVD.BEGIN_DTM,
CURRENT_TIMESTAMP AS ROW_PROCESS_DTM, 
DRVD.AUDIT_ID, DRVD.LOGICAL_DELETE_IND, DRVD.CHECK_SUM,
CASE WHEN ROW_NUMBER() OVER (PARTITION BY DRVD.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID ORDER BY DRVD.END_DTM DESC, DRVD.END_DT DESC) = 1 AND DRVD.END_DT = '9999-12-31' THEN 'T' ELSE 'F' END::BOOLEAN AS CURRENT_ROW_IND,
DRVD.END_DT, DRVD.END_DTM, DRVD.SOURCE_SYSTEM_ID, DRVD.RESTRICTED_ROW_IND
FROM (SELECT UUID_GEN(PREHASH_VALUE(AGREEMENT_SOURCE_CDE, AGREEMENT_TYPE_CDE, AGREEMENT_NR_PFX, AGREEMENT_NR, AGREEMENT_NR_SFX))::UUID AS DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
		, UUID_GEN(PREHASH_VALUE(PARTY_ID))::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID
		, UUID_GEN(PREHASH_VALUE(PARTY_ROLE_CDE))::UUID AS REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID
		, DECODE(PRODUCT_ID,NULL,NULL,UUID_GEN(PREHASH_VALUE(PRODUCT_ID))) AS DIM_PRODUCT_NATURAL_KEY_HASH_UUID
		, CLEAN_STRING('') AS DIM_ACCOUNT_NATURAL_KEY_HASH_UUID
		, CLEAN_STRING('') AS REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID
		, HASH_GEN(PREHASH_VALUE(PRODUCT_ID,CLEAN_STRING(''),CLEAN_STRING('')))::UUID AS CHECK_SUM
		, ADT.AUDIT_ID AS AUDIT_ID
		, '203' AS SOURCE_SYSTEM_ID
		, 'F'::BOOLEAN AS RESTRICTED_ROW_IND
		, 'F'::BOOLEAN AS LOGICAL_DELETE_IND
		, T1.RCRD_EFFCTV_DT AS BEGIN_DTM
		, T1.RCRD_EFFCTV_DT AS BEGIN_DT
		, CURRENT_TIMESTAMP AS ROW_PROCESS_DTM
		, T1.RCRD_TRM_DT AS END_DT
		, T1.RCRD_TRM_DT AS END_DTM
		, 'T'::BOOLEAN AS CURRENT_ROW_IND
	FROM (SELECT DISTINCT CLEAN_STRING('') AS AGREEMENT_NR_PFX
			, PREHASH_VALUE(CLEAN_STRING(PRT.ICU_ID),CLEAN_STRING(TO_CHAR(PRT.SPNSR_CMPNY_ID::INTEGER)),CLEAN_STRING(PRT.PLAN_ID),
			CLEAN_STRING(PRT.PRTCPNT_ID) ) AS AGREEMENT_NR
			, CLEAN_STRING('') AS AGREEMENT_NR_SFX
			, CLEAN_STRING('TRAC') AS AGREEMENT_SOURCE_CDE
			, CLEAN_STRING('PCA') AS AGREEMENT_TYPE_CDE
			, PREHASH_VALUE(CLEAN_STRING('TRAC_ENTITY'),CLEAN_STRING(PRT.ICU_ID),CLEAN_STRING(TO_CHAR(PRT.SPNSR_CMPNY_ID::INTEGER))) AS PARTY_ID
			, CLEAN_STRING('SPNSR') AS PARTY_ROLE_CDE
			, CLEAN_STRING(PROD.PROD_ID) AS PRODUCT_ID
			, '9999-12-31'::timestamp AS RCRD_TRM_DT
			, '0001-01-01'::timestamp AS RCRD_EFFCTV_DT
		FROM TRAC_RPGW.DIM_PARTICIPANT PRT

		LEFT OUTER JOIN TRAC_RPGW.DIM_PLAN PLAN
		ON PRT.ICU_ID = PLAN.ICU_ID
		AND PRT.PLAN_ID = PLAN.PLAN_ID

		LEFT OUTER JOIN (SELECT DISTINCT UPPER(KND_MIN_CDE) AS KND_MIN_CDE, PROD_ID FROM 
		TRAC_RPGW.PRODUCT_TRANSLATOR_VW WHERE UPPER(TRIM(ADMN_SYS_CDE)) = 'TRAC') PROD
		ON PROD.KND_MIN_CDE = UPPER(PLAN.PLAN_TYPE_CD)
		WHERE UPPER(PRT.ACTV_RCRD_IND) = 'Y' AND UPPER(PLAN.ACTV_RCRD_IND) = 'Y') T1
		
	LEFT JOIN EDW_AUDIT.ETL_BATCH_AUDIT_VW ADT
	ON ADT.RECORD_ALIGNER = 1
	AND ADT.BATCH_NAME = 'Trac_to_CIP') DRVD
	
LEFT JOIN (SELECT DIM_AGREEMENT_NATURAL_KEY_HASH_UUID::UUID AS DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, DIM_PARTY_NATURAL_KEY_HASH_UUID::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID, REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID::UUID AS REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID, END_DT, CURRENT_ROW_IND, CHECK_SUM::UUID AS CHECK_SUM, SOURCE_SYSTEM_ID
	FROM EDW_WORK.TRAC_REL_PARTY_AGREEMENT) TGT
ON TGT.CURRENT_ROW_IND= TRUE
AND TGT.END_DT='9999-12-31'
AND TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID = DRVD.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
AND TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID = DRVD.DIM_PARTY_NATURAL_KEY_HASH_UUID
AND TGT.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID = DRVD.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID

WHERE CASE WHEN TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID IS NULL THEN 'I'
	WHEN TGT.CHECK_SUM <> DRVD.CHECK_SUM THEN 'U'
	ELSE 'D' END IN ('I','U');

/* INSERT AND UPDATE NEW RECORDS OF MASTER CONTRACT PARTICIPANT*/
INSERT INTO EDW_WORK.TRAC_REL_PARTY_AGREEMENT (DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, DIM_PARTY_NATURAL_KEY_HASH_UUID, REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID, REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,DIM_PRODUCT_NATURAL_KEY_HASH_UUID,DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
	BEGIN_DT, BEGIN_DTM, ROW_PROCESS_DTM, AUDIT_ID, LOGICAL_DELETE_IND, CHECK_SUM, CURRENT_ROW_IND, END_DT, END_DTM, SOURCE_SYSTEM_ID, RESTRICTED_ROW_IND)
SELECT DRVD.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, DRVD.DIM_PARTY_NATURAL_KEY_HASH_UUID, DRVD.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID, DRVD.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID, DRVD.DIM_PRODUCT_NATURAL_KEY_HASH_UUID,
NULL AS DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
DRVD.BEGIN_DT, DRVD.BEGIN_DTM,
CURRENT_TIMESTAMP AS ROW_PROCESS_DTM,
DRVD.AUDIT_ID, DRVD.LOGICAL_DELETE_IND, DRVD.CHECK_SUM,
CASE WHEN ROW_NUMBER() OVER (PARTITION BY DRVD.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID ORDER BY DRVD.END_DTM DESC, DRVD.END_DT DESC) = 1 AND DRVD.END_DT = '9999-12-31' THEN 'T' ELSE 'F' END::BOOLEAN AS CURRENT_ROW_IND,
DRVD.END_DT, DRVD.END_DTM, DRVD.SOURCE_SYSTEM_ID, DRVD.RESTRICTED_ROW_IND
FROM (SELECT UUID_GEN(PREHASH_VALUE(AGREEMENT_SOURCE_CDE, AGREEMENT_TYPE_CDE, AGREEMENT_NR_PFX, AGREEMENT_NR, AGREEMENT_NR_SFX))::UUID AS DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
	, UUID_GEN(PREHASH_VALUE(PARTY_ID))::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID
	, UUID_GEN(PREHASH_VALUE(PARTY_ROLE_CDE))::UUID AS REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID
	, DECODE(PRODUCT_ID,NULL,NULL,UUID_GEN(PREHASH_VALUE(PRODUCT_ID))) AS DIM_PRODUCT_NATURAL_KEY_HASH_UUID
	, UUID_GEN(PREHASH_VALUE(PARTY_ROLE_CDE,PARTY_ID, AGREEMENT_SOURCE_CDE, AGREEMENT_TYPE_CDE, AGREEMENT_NR_PFX, AGREEMENT_NR, AGREEMENT_NR_SFX)) AS DIM_ACCOUNT_NATURAL_KEY_HASH_UUID
	, DECODE(PARTY_SUB_ROLE_CDE,NULL,NULL,UUID_GEN(PREHASH_VALUE(PARTY_SUB_ROLE_CDE))) AS REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID
	, HASH_GEN(PREHASH_VALUE(PRODUCT_ID,PARTY_ROLE_CDE,PARTY_ID, AGREEMENT_SOURCE_CDE, AGREEMENT_TYPE_CDE, AGREEMENT_NR_PFX, AGREEMENT_NR, AGREEMENT_NR_SFX,PARTY_SUB_ROLE_CDE))::UUID AS CHECK_SUM
	, ADT.AUDIT_ID AS AUDIT_ID
	, '203' AS SOURCE_SYSTEM_ID
	, 'F'::BOOLEAN AS RESTRICTED_ROW_IND
	, 'F'::BOOLEAN AS LOGICAL_DELETE_IND
	, T1.RCRD_EFFCTV_DT AS BEGIN_DTM
	, T1.RCRD_EFFCTV_DT::date AS BEGIN_DT
	, CURRENT_TIMESTAMP AS ROW_PROCESS_DTM
	, T1.RCRD_TRM_DT::date AS END_DT
	, T1.RCRD_TRM_DT AS END_DTM
	, 'T'::BOOLEAN AS CURRENT_ROW_IND
	FROM (SELECT DISTINCT CLEAN_STRING('') AS AGREEMENT_NR_PFX
		, PREHASH_VALUE(CLEAN_STRING(PRT.ICU_ID),CLEAN_STRING(TO_CHAR(PRT.SPNSR_CMPNY_ID::INTEGER)),CLEAN_STRING(PRT.PLAN_ID),CLEAN_STRING(PRT.PRTCPNT_ID) ) AS AGREEMENT_NR
		, CLEAN_STRING('') AS AGREEMENT_NR_SFX
		, CLEAN_STRING('TRAC') AS AGREEMENT_SOURCE_CDE
		, CLEAN_STRING('PCA') AS AGREEMENT_TYPE_CDE
		,  XREF.PARTY_ID AS PARTY_ID
		, CLEAN_STRING('PTCP') AS PARTY_ROLE_CDE
		, CLEAN_STRING(PROD.PROD_ID) AS PRODUCT_ID
		, CLEAN_STRING(SDT1.TRNSLT_FLD_VAL) AS PARTY_SUB_ROLE_CDE
		, '9999-12-31'::timestamp AS RCRD_TRM_DT
		, '0001-01-01'::timestamp AS RCRD_EFFCTV_DT
		FROM (SELECT DISTINCT PARTY_ID, SOR_PARTY_ID FROM EDW.PARTY_MASTER_OF_MASTERS_XREF WHERE PARTY_ID_TYPE_CDE = 'TRAC_PRTC_ID' AND LOGICAL_DELETE_IND = 'F'::BOOLEAN) XREF

		JOIN TRAC_RPGW.DIM_PARTICIPANT PRT
		ON XREF.SOR_PARTY_ID = CLEAN_STRING(PRT.ICU_ID||'_'||PRT.PLAN_ID||'_'||PRT.PRTCPNT_ID)

		LEFT OUTER JOIN TRAC_RPGW.DIM_PLAN PLAN
		ON PRT.ICU_ID = PLAN.ICU_ID
		AND PRT.PLAN_ID = PLAN.PLAN_ID

		LEFT OUTER JOIN (SELECT DISTINCT UPPER(TRIM(KND_MIN_CDE)) AS KND_MIN_CDE, PROD_ID FROM TRAC_RPGW.PRODUCT_TRANSLATOR_VW WHERE UPPER(TRIM(ADMN_SYS_CDE)) = 'TRAC') PROD
		ON PROD.KND_MIN_CDE = UPPER(TRIM(PLAN.PLAN_TYPE_CD))

		LEFT OUTER JOIN (SELECT UPPER(TRIM(SRC_FLD_VAL)) AS SRC_FLD_VAL, TRNSLT_FLD_VAL FROM CIP_SOURCE.SRC_DATA_TRNSLT_VW WHERE UPPER(TRIM(SRC_CDE)) = 'TRAC' AND UPPER(TRIM(SRC_TBL_NM)) = 'DIM_PARTICIPANT' AND UPPER(TRIM(SRC_FLD_NM)) = 'PRTCPNT_STTS_CD' AND UPPER(TRIM(TRNSLT_FLD_NM)) = 'ROLE CATEGORY') SDT1
		ON SRC_FLD_VAL = UPPER(TRIM(PRT.PRTCPNT_STTS_CD))

		WHERE UPPER(TRIM(PRT.ACTV_RCRD_IND)) = 'Y' AND UPPER(TRIM(PLAN.ACTV_RCRD_IND)) = 'Y' ) T1

	LEFT JOIN EDW_AUDIT.ETL_BATCH_AUDIT_VW ADT
	ON ADT.RECORD_ALIGNER = 1
	AND ADT.BATCH_NAME = 'Trac_to_CIP') DRVD

LEFT JOIN (SELECT DIM_AGREEMENT_NATURAL_KEY_HASH_UUID::UUID AS DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, DIM_PARTY_NATURAL_KEY_HASH_UUID::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID, REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID::UUID AS REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID, END_DT, CURRENT_ROW_IND, CHECK_SUM::UUID AS CHECK_SUM, SOURCE_SYSTEM_ID
	FROM EDW_WORK.TRAC_REL_PARTY_AGREEMENT) TGT
ON TGT.CURRENT_ROW_IND= TRUE
AND TGT.END_DT='9999-12-31'
AND TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID = DRVD.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
AND TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID = DRVD.DIM_PARTY_NATURAL_KEY_HASH_UUID
AND TGT.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID = DRVD.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID

WHERE CASE WHEN TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID IS NULL THEN 'I'
	WHEN TGT.CHECK_SUM <> DRVD.CHECK_SUM THEN 'U'
	ELSE 'D' END IN ('I','U');

/* INSERT AND UPDATE NEW RECORDS OF MASTER CONTRACT BENEFICIARY*/
INSERT INTO EDW_WORK.TRAC_REL_PARTY_AGREEMENT (DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, DIM_PARTY_NATURAL_KEY_HASH_UUID, REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID, REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID,DIM_PRODUCT_NATURAL_KEY_HASH_UUID,DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
	BEGIN_DT, BEGIN_DTM, ROW_PROCESS_DTM, AUDIT_ID, LOGICAL_DELETE_IND, CHECK_SUM, CURRENT_ROW_IND, END_DT, END_DTM, SOURCE_SYSTEM_ID, RESTRICTED_ROW_IND)	
SELECT DRVD.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, DRVD.DIM_PARTY_NATURAL_KEY_HASH_UUID, DRVD.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID, DRVD.REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID, DRVD.DIM_PRODUCT_NATURAL_KEY_HASH_UUID,
NULL AS DIM_ACCOUNT_NATURAL_KEY_HASH_UUID,
DRVD.BEGIN_DT, DRVD.BEGIN_DTM,
CURRENT_TIMESTAMP AS ROW_PROCESS_DTM, 
DRVD.AUDIT_ID, DRVD.LOGICAL_DELETE_IND, DRVD.CHECK_SUM,
CASE WHEN ROW_NUMBER() OVER (PARTITION BY DRVD.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID ORDER BY DRVD.END_DTM DESC, DRVD.END_DT DESC) = 1 AND DRVD.END_DT = '9999-12-31' THEN 'T' ELSE 'F' END::BOOLEAN AS CURRENT_ROW_IND,
DRVD.END_DT, DRVD.END_DTM, DRVD.SOURCE_SYSTEM_ID, DRVD.RESTRICTED_ROW_IND
FROM (SELECT UUID_GEN(PREHASH_VALUE(AGREEMENT_SOURCE_CDE, AGREEMENT_TYPE_CDE, AGREEMENT_NR_PFX, AGREEMENT_NR, AGREEMENT_NR_SFX))::UUID AS DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
		, UUID_GEN(PREHASH_VALUE(PARTY_ID))::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID
		, UUID_GEN(PREHASH_VALUE(PARTY_ROLE_CDE))::UUID AS REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID
		, DECODE(PRODUCT_ID,NULL,NULL,UUID_GEN(PREHASH_VALUE(PRODUCT_ID))) AS DIM_PRODUCT_NATURAL_KEY_HASH_UUID
		, CLEAN_STRING('') AS DIM_ACCOUNT_NATURAL_KEY_HASH_UUID
		, CLEAN_STRING('') AS REF_PARTY_SUB_ROLE_NATURAL_KEY_HASH_UUID
		, HASH_GEN(PREHASH_VALUE(PRODUCT_ID,CLEAN_STRING(''),CLEAN_STRING('')))::UUID AS CHECK_SUM
		, ADT.AUDIT_ID AS AUDIT_ID
		, '203' AS SOURCE_SYSTEM_ID
		, 'F'::BOOLEAN AS RESTRICTED_ROW_IND
		, 'F'::BOOLEAN AS LOGICAL_DELETE_IND
		, T1.RCRD_EFFCTV_DT AS BEGIN_DTM
		, T1.RCRD_EFFCTV_DT::date AS BEGIN_DT
		, CURRENT_TIMESTAMP AS ROW_PROCESS_DTM
		, T1.RCRD_TRM_DT::date AS END_DT
		, T1.RCRD_TRM_DT AS END_DTM
		, 'T'::BOOLEAN AS CURRENT_ROW_IND
	FROM (SELECT DISTINCT CLEAN_STRING('') AS AGREEMENT_NR_PFX
			, PREHASH_VALUE(CLEAN_STRING(BEN.ICU_ID),CLEAN_STRING(TO_CHAR(PLAN.SPNSR_CMPNY_ID::INTEGER)),CLEAN_STRING(PLAN.PLAN_ID),CLEAN_STRING(BEN.PRTCPNT_ID) ) AS AGREEMENT_NR
			, CLEAN_STRING('') AS AGREEMENT_NR_SFX
			, CLEAN_STRING('TRAC') AS AGREEMENT_SOURCE_CDE
			, CLEAN_STRING('PCA') AS AGREEMENT_TYPE_CDE
			, XREF.PARTY_ID AS PARTY_ID
			, CLEAN_STRING('BENE') AS PARTY_ROLE_CDE
			, CLEAN_STRING(PROD.PROD_ID) AS PRODUCT_ID
			, '9999-12-31'::timestamp AS RCRD_TRM_DT
			, '0001-01-01'::timestamp AS RCRD_EFFCTV_DT
		FROM (SELECT DISTINCT PARTY_ID, SOR_PARTY_ID FROM EDW.PARTY_MASTER_OF_MASTERS_XREF WHERE PARTY_ID_TYPE_CDE = 'TRAC_BEN_ID' AND LOGICAL_DELETE_IND = 'F'::BOOLEAN) XREF

		JOIN TRAC_RPGW.DIM_BENEFICIARY BEN
		ON CLEAN_STRING(XREF.SOR_PARTY_ID) =  CLEAN_STRING(BEN.ICU_ID||'_'||BEN.PLAN_ID||'_'||BEN.PRTCPNT_ID||'_'||BEN.BNFCRY_ID)

		LEFT OUTER JOIN TRAC_RPGW.DIM_PLAN PLAN
		ON BEN.ICU_ID = PLAN.ICU_ID
		AND BEN.PLAN_ID = PLAN.PLAN_ID

		LEFT OUTER JOIN (SELECT DISTINCT UPPER(TRIM(KND_MIN_CDE)) AS KND_MIN_CDE, PROD_ID FROM TRAC_RPGW.PRODUCT_TRANSLATOR_VW WHERE UPPER(TRIM(ADMN_SYS_CDE)) = 'TRAC') PROD
		ON PROD.KND_MIN_CDE = UPPER(TRIM(PLAN.PLAN_TYPE_CD))

		WHERE UPPER(TRIM(BEN.ACTV_RCRD_IND)) = 'Y' AND UPPER(TRIM(PLAN.ACTV_RCRD_IND)) = 'Y') T1

	LEFT JOIN EDW_AUDIT.ETL_BATCH_AUDIT_VW ADT
	ON ADT.RECORD_ALIGNER = 1
	AND ADT.BATCH_NAME = 'Trac_to_CIP') DRVD

LEFT JOIN (SELECT DIM_AGREEMENT_NATURAL_KEY_HASH_UUID::UUID AS DIM_AGREEMENT_NATURAL_KEY_HASH_UUID, DIM_PARTY_NATURAL_KEY_HASH_UUID::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID, REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID::UUID AS REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID, END_DT, CURRENT_ROW_IND, CHECK_SUM::UUID AS CHECK_SUM, SOURCE_SYSTEM_ID
	FROM EDW_WORK.TRAC_REL_PARTY_AGREEMENT) TGT
ON TGT.CURRENT_ROW_IND= TRUE
AND TGT.END_DT='9999-12-31'
AND TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID = DRVD.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID
AND TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID = DRVD.DIM_PARTY_NATURAL_KEY_HASH_UUID
AND TGT.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID = DRVD.REF_PARTY_ROLE_NATURAL_KEY_HASH_UUID

WHERE CASE WHEN TGT.DIM_AGREEMENT_NATURAL_KEY_HASH_UUID IS NULL THEN 'I'
	WHEN TGT.CHECK_SUM <> DRVD.CHECK_SUM THEN 'U'
	ELSE 'D' END IN ('I','U');
