WITH client AS (
    SELECT *
    FROM {{ source('alaya_care', 'client') }}
    WHERE _etl_is_deleted = FALSE
),

flatten_cols AS (
    SELECT
        client_id AS ac_client_id,
        profile_id,
        guid,
        branch_id,
        profile:comcare_id::VARCHAR AS comcare_id,
        profile:uid::VARCHAR AS external_id,
        profile:salutation::VARCHAR AS salutation,
        profile:first_name::VARCHAR AS first_name, -- noqa: RF04
        profile:middlename::VARCHAR AS middle_name, -- noqa: RF04
        profile:last_name::VARCHAR AS last_name, -- noqa: RF04
        profile:preferred_name::VARCHAR AS preferred_name,
        birthday AS birth_date,
        profile:marital_status::VARCHAR AS marital_status,
        profile:religion::VARCHAR AS religion,
        profile:country_of_birth_code::VARCHAR AS country_of_birth,
        preferred_language,
        profile:language_spoken_at_home_code::VARCHAR
            AS language_spoken_at_home_code,
        profile:client_email_comments::VARCHAR AS email_comments,
        profile:email::VARCHAR AS email, -- noqa: RF04
        profile:client_phone_comments::VARCHAR AS phone_comments,
        profile:phone_main::VARCHAR AS phone_main,
        profile:phone_personal::VARCHAR AS phone_personal,
        profile:phone_other::VARCHAR AS phone_other,
        profile:fax::VARCHAR AS fax,
        profile:pension_status::VARCHAR AS pension_status,
        profile:address_suite::VARCHAR AS address_suite,
        profile:address::VARCHAR AS address,
        profile:city::VARCHAR AS city,
        profile:state::VARCHAR AS state,
        profile:zip::VARCHAR AS postcode,
        profile:country::VARCHAR AS country,
        profile:postal_address_line_1::VARCHAR AS postal_address_line_1,
        profile:postal_suburb::VARCHAR AS postal_suburb,
        profile:postal_state::VARCHAR AS postal_state,
        profile:postal_postcode::VARCHAR AS postal_postcode,
        profile:postal_country::VARCHAR AS postal_country,
        profile:mailing_address_1::VARCHAR AS mailing_address_1,
        profile:mailing_address_2::VARCHAR AS mailing_address_2,
        profile:mailing_suburb::VARCHAR AS mailing_suburb,
        profile:mailing_state::VARCHAR AS mailing_state,
        profile:mailing_post_code::VARCHAR AS mailing_post_code,
        profile:mailing_country::VARCHAR AS mailing_country,
        profile:billing_address_1::VARCHAR AS billing_address_1,
        profile:billing_address_2::VARCHAR AS billing_address_2,
        profile:billing_suburb::VARCHAR AS billing_suburb,
        profile:billing_state::VARCHAR AS billing_state,
        profile:billing_post_code::VARCHAR AS billing_postcode,
        profile:billing_country::VARCHAR AS billing_country,
        profile:hcp_approval_date::DATE AS hcp_approval_date,
        profile:account_contact_name::VARCHAR AS account_contact_name,
        profile:sms_contact_name::VARCHAR AS sms_contact_name,
        profile:sms_contact_number::VARCHAR AS sms_contact_number,
        profile:client_custom_stmt_delivery::VARCHAR
            AS client_custom_stmt_delivery,
        profile:date_of_death::DATE AS deceased_date,
        profile:time_of_death::VARCHAR AS time_of_death,
        profile:death_location_category::VARCHAR
            AS death_location_category,
        profile:disability_code::VARCHAR AS disability_code,
        profile:medicare_number::VARCHAR AS medicare_number,
        profile:medicare_card_no::VARCHAR AS medicare_card_no,
        profile:medicare_reference::VARCHAR AS medicare_reference,
        profile:medicare_card_expiry::VARCHAR AS medicare_card_expiry,
        profile:ech_site_location_name::VARCHAR
            AS ech_site_location_name,
        profile:client_ndis_number::VARCHAR AS client_ndis_number,
        profile:aboriginal_or_torres_strait_islander_origin_code::VARCHAR
            AS is_first_nation,
        profile:fastcard::VARCHAR AS fastcard,
        profile:dva_ref_prov_num::VARCHAR AS dva_ref_prov_num,
        profile:dva_card_number::VARCHAR AS dva_card_number,
        profile:dva_ref_date::VARCHAR AS dva_ref_date,
        profile:dva_entry_date::VARCHAR AS dva_entry_date,
        profile:centrelink_reference_number::VARCHAR
            AS centrelink_reference_number,
        profile:private_health_insurance::VARCHAR
            AS private_health_insurance,
        profile:is_billing_contact::VARCHAR AS is_billing_contact,
        profile:accommodation_type_code::VARCHAR AS accommodation_type,
        profile:is_a_carer::VARCHAR AS is_a_carer,
        profile:education_level_code::VARCHAR AS education_level,
        profile:tags_v2::VARCHAR AS tags_v_2,
        profile:has_carer::VARCHAR AS has_carer,
        profile:carer_residency::VARCHAR AS carer_residency,
        profile:remarks::VARCHAR AS remarks,
        profile:client_is_tpi::VARCHAR AS client_is_tpi,
        admission_date::DATE AS admission_date,
        discharge_date::DATE AS discharged_date,
        client_brn,
        visit_count,
        has_adls,
        created_at,
        created_by,
        updated_at,
        updated_by,
        'AC' || LPAD(guid, 9, '0') AS alayacare_id,
        NULLIF(profile:mac_id::VARCHAR, '') AS mac_id,
        NULLIF(profile:hcp_recipient_id::VARCHAR, '')
            AS hcp_recipient_id,
        NULLIF(profile:import_id::VARCHAR, '') AS import_id,
        NULLIF(profile:d365_debtor_id::VARCHAR, '') AS d_365_debtor_id,
        COALESCE(profile:is_birth_date_an_estimate::BOOLEAN, FALSE)
            AS is_birth_date_an_estimate,
        COALESCE(NULLIF(profile:gender_code::VARCHAR, ''), 'Not Stated')
            AS gender,
        INITCAP(properties_tbl_gt_account:idstatus::VARCHAR)
            AS client_status,
        COALESCE(profile:interpreter_required::BOOLEAN, FALSE)
            AS interpreter_required,
        COALESCE(profile:ech_retirement_living_resident::BOOLEAN, FALSE)
            AS ech_retirement_living_resident,
        NULLIF(profile:ambulance_card_number::VARCHAR, '')
            AS ambulance_card_number,
        NULLIF(profile:carer_contact_name_primary_only::VARCHAR, '')
            AS carer_contact_name,
        COALESCE(profile:ech_assisted_living_resident::BOOLEAN, FALSE)
            AS ech_assisted_living_resident,
        NULLIF(
            profile:hcp_client_discharged_to_another_provider::VARCHAR,
            ''
        )
            AS hcp_client_discharged_to_another_provider,
        NULLIF(
            profile:aboriginal_or_torres_strait_islander_origin_code::VARCHAR,
            ''
        ) AS atsi_status,
        COALESCE(profile:is_using_pseudonym::BOOLEAN, FALSE)
            AS is_using_pseudonym,
        COALESCE(
            profile:consent_to_record_lgbtiqa_status::BOOLEAN, FALSE
        )
            AS consent_to_record_lgbtiqa_status,
        COALESCE(profile:consent_for_future_contacts::BOOLEAN, FALSE)
            AS consent_for_future_contacts,
        COALESCE(profile:consent_to_share_information::BOOLEAN, FALSE)
            AS consent_to_share_information,
        NULLIF(profile:dva_card_type::VARCHAR, '') AS dva_card_type,
        NULLIF(profile:household_composition_code::VARCHAR, '')
            AS living_arrangements,
        NULLIF(profile:ace::VARCHAR, '') AS ace,
        NULLIF(profile:executor_contact_name::VARCHAR, '')
            AS executor_contact_name,
        NULLIF(
            profile:hcp_client_transfer_from_another_provider::VARCHAR,
            ''
        )
            AS hcp_client_transfer_from_another_provider
    FROM client
)

SELECT *
FROM flatten_cols
