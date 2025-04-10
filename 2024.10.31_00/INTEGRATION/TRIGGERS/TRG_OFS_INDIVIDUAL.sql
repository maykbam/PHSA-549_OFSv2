--liquibase formatted.sql
--changeset michael.cawayan:INTEGRATION.TRG_OFS_INDIVIDUAL contextFilter:PH endDelimiter:/ runOnChange:true

create or replace TRIGGER integration.trg_ofs_individual 
INSTEAD OF
    INSERT ON integration.vw_ofs_individual
FOR EACH ROW
DECLARE
    temp_chid NUMBER;
BEGIN
    IF NVL(:new.offerId,'') = '' OR NVL(:new.offerIdSas,'') = '' THEN
		RETURN;
    END IF;
    BEGIN
	SELECT ch_id
	INTO temp_chid
	FROM MONITOR.RTDM_LOG_ACTION_OFFER
	WHERE RTDM_LOG_EVENT_ID = :new.offerIdSas;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            temp_chid := NULL;
	WHEN TOO_MANY_ROWS THEN
	    temp_chid := NULL;
    END;
    IF temp_chid IS NULL THEN
		RETURN;
    END IF;
	
    IF :new.status = 'CREATED' THEN
		UPDATE int_os.os_output_load
		SET
			processed_flg = 1,
			offer_id = :new.offerId
		WHERE CH_ID = temp_chid;
	
    	UPDATE cdm.ci_contact_history_offer
		SET
			contact_history_status_cd = '_60'
		WHERE external_contact_info_id1 = TO_CHAR(temp_chid);
	
    	UPDATE cdm.ci_contact_history_ext_offer
		SET
			offer_id = :new.offerId
		WHERE ch_id = temp_chid;

        UPDATE MONITOR.RTDM_LOG_ACTION_OFFER
		SET
			offer_id = :new.offerId
		WHERE ch_id = temp_chid;
	
    ELSE
		UPDATE int_os.os_output_load
		SET
			processed_flg = 0
		WHERE ch_id = TO_CHAR(temp_chid);
		UPDATE cdm.ci_contact_history_offer
		SET
			contact_history_status_cd = '_70'
		WHERE external_contact_info_id1 = TO_CHAR(temp_chid);
		UPDATE cdm.ci_contact_history_ext_offer
		SET
			offer_id = :new.offerId
		WHERE ch_id = temp_chid;
	END IF;
END;
/
ALTER TRIGGER "INTEGRATION"."TRG_OFS_INDIVIDUAL" ENABLE
/
GRANT SELECT ON "INTEGRATION"."VW_OFS_INDIVIDUAL" TO "HCI_RO_INTEGRATION"
/
GRANT DELETE ON "INTEGRATION"."VW_OFS_INDIVIDUAL" TO "ESP_USER"
/
GRANT INSERT ON "INTEGRATION"."VW_OFS_INDIVIDUAL" TO "ESP_USER"
/
GRANT SELECT ON "INTEGRATION"."VW_OFS_INDIVIDUAL" TO "ESP_USER"
/
GRANT UPDATE ON "INTEGRATION"."VW_OFS_INDIVIDUAL" TO "ESP_USER"
/
GRANT SELECT ON "INTEGRATION"."VW_OFS_INDIVIDUAL" TO "SAS_INTEGRATION_RO"
/
GRANT SELECT, UPDATE ON MONITOR.RTDM_LOG_ACTION_OFFER TO INTEGRATION
/
GRANT UPDATE ON int_os.os_output_load TO INTEGRATION
/
GRANT UPDATE ON cdm.ci_contact_history_offer TO INTEGRATION
/
GRANT UPDATE ON cdm.ci_contact_history_ext_offer TO INTEGRATION
/
GRANT SELECT, INSERT, UPDATE, DELETE ON "INTEGRATION"."VW_OFS_INDIVIDUAL" TO MA_TEMP
/