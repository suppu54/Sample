/* TRUNCATION OF THE WORK TABLE */

TRUNCATE TABLE EDW_WORK.TRAC_REL_PARTY_ELECTRONIC_ADDRESS;

/* TRAC SQL QUERY TO CAPTURE BOTH INSERT AND UPDATE RECORDS */

INSERT INTO EDW_WORK.TRAC_REL_PARTY_ELECTRONIC_ADDRESS
(REF_ELECTRONIC_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID, DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID, REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID, DIM_PARTY_NATURAL_KEY_HASH_UUID, ELECTRONIC_ADDRESS_INVALID_IND, PRIMARY_ELECTRONIC_ADDRESS_IND, BEGIN_DT, BEGIN_DTM, ROW_PROCESS_DTM, AUDIT_ID, LOGICAL_DELETE_IND, CHECK_SUM, END_DT, END_DTM, SOURCE_SYSTEM_ID, RESTRICTED_ROW_IND, CURRENT_ROW_IND)

SELECT SRC.REF_ELECTRONIC_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID, SRC.DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID, SRC.REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID, SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID, SRC.ELECTRONIC_ADDRESS_INVALID_IND, SRC.PRIMARY_ELECTRONIC_ADDRESS_IND, SRC.BEGIN_DT, SRC.BEGIN_DTM, CURRENT_TIMESTAMP as ROW_PROCESS_DTM, SRC.AUDIT_ID, SRC.LOGICAL_DELETE_IND, SRC.CHECK_SUM, SRC.END_DT, SRC.END_DTM, SRC.SOURCE_SYSTEM_ID, SRC.RESTRICTED_ROW_IND
	, CASE WHEN ROW_NUMBER() OVER (PARTITION BY SRC.REF_ELECTRONIC_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID , SRC.DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID, SRC.REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID, SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID ORDER BY SRC.END_DTM desc, SRC.END_DT desc) = 1 and SRC.END_DT = '9999-12-31' THEN 'T' ELSE 'F' END::BOOLEAN AS CURRENT_ROW_IND
	FROM (
		SELECT DISTINCT HASH_GEN(PREHASH_VALUE(CLEAN_STRING('SRV')))::UUID AS REF_ELECTRONIC_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID
		, HASH_GEN(PREHASH_VALUE(CLEAN_STRING(STM_EML_ADR_TXT)))::UUID AS DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID
		, HASH_GEN(PREHASH_VALUE(CLEAN_STRING('TRAC')))::UUID AS REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID
		, HASH_GEN(PREHASH_VALUE(PARTY_ID))::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID
		, NULL AS ELECTRONIC_ADDRESS_INVALID_IND
		, NULL AS PRIMARY_ELECTRONIC_ADDRESS_IND
		, CAST(RCRD_EFFCTV_DT AS DATE)::date as BEGIN_DT
		, CAST(RCRD_EFFCTV_DT AS DATE)::timestamp as BEGIN_DTM
		, ADT.AUDIT_ID AS AUDIT_ID
		, 'F'::boolean AS LOGICAL_DELETE_IND
		, HASH_GEN(PREHASH_VALUE(CLEAN_STRING(NULL), CLEAN_STRING(NULL)))::UUID AS CHECK_SUM
		, CAST(RCRD_TRM_DT AS DATE)::date as END_DT
		, CAST(RCRD_TRM_DT AS DATE)::timestamp as END_DTM
		, '203' AS SOURCE_SYSTEM_ID
		, 'F'::boolean AS RESTRICTED_ROW_IND
		FROM TRAC_RPGW.DIM_CONSENT_CUST TRC
LEFT OUTER JOIN EDW_AUDIT.ETL_BATCH_AUDIT_VW ADT
	ON ADT.BATCH_NAME = 'Trac_to_CIP'
	AND ADT.RECORD_ALIGNER =1
JOIN (SELECT DISTINCT PARTY_ID, SOR_PARTY_ID FROM EDW.PARTY_MASTER_OF_MASTERS_XREF WHERE PARTY_ID_TYPE_CDE = 'TRAC_CUST_ID' AND LOGICAL_DELETE_IND = 'F'::BOOLEAN) XREF
	ON PREHASH_VALUE(CLEAN_STRING(CUSTOMER_ID1),CLEAN_STRING(CUSTOMER_ID2)) = XREF.SOR_PARTY_ID
	WHERE UPPER(BTRIM(TRC.ACTV_RCRD_IND)) = 'Y' 

 ) SRC
LEFT OUTER JOIN (SELECT REF_ELECTRONIC_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID::UUID AS REF_ELECTRONIC_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID, DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID::UUID AS DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID, REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID::UUID AS REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID, DIM_PARTY_NATURAL_KEY_HASH_UUID::UUID AS DIM_PARTY_NATURAL_KEY_HASH_UUID, CHECK_SUM::UUID AS CHECK_SUM, CURRENT_ROW_IND, END_DT 
FROM EDW_WORK.TRAC_REL_PARTY_ELECTRONIC_ADDRESS)TGT
	ON TGT.CURRENT_ROW_IND= TRUE
	AND TGT.END_DT='9999-12-31'
	AND TGT.REF_ELECTRONIC_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID=SRC.REF_ELECTRONIC_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID
	AND TGT.DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID=SRC.DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID
	AND TGT.REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID=SRC.REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID
	AND TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID=SRC.DIM_PARTY_NATURAL_KEY_HASH_UUID

WHERE CASE WHEN TGT.REF_ELECTRONIC_ADDRESS_TYPE_NATURAL_KEY_HASH_UUID IS NULL OR TGT.DIM_ELECTRONIC_ADDRESS_NATURAL_KEY_HASH_UUID IS NULL OR TGT.REF_PARTY_CONTACT_SOURCE_NATURAL_KEY_HASH_UUID IS NULL OR TGT.DIM_PARTY_NATURAL_KEY_HASH_UUID IS NULL THEN 'I'
	WHEN TGT.CHECK_SUM <> SRC.CHECK_SUM THEN 'U'
	ELSE 'D' END IN ('I','U');
