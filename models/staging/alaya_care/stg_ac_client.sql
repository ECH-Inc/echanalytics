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
        profile:mac_id::VARCHAR AS mac_id,
        profile:hcp_recipient_id::VARCHAR AS hcp_recipient_id,
        profile:import_id::VARCHAR AS import_id,
        profile:uid::VARCHAR AS crm_id,
        profile:salutation::VARCHAR AS salutation,
        profile:first_name::VARCHAR AS first_name, -- noqa: RF04
        profile:middlename::VARCHAR AS middle_name, -- noqa: RF04
        profile:last_name::VARCHAR AS last_name, -- noqa: RF04
        profile:preferred_name::VARCHAR AS preferred_name,
        birthday,
        profile:is_birth_date_an_estimate::VARCHAR AS is_birth_date_an_estimate,
        profile:gender_code::VARCHAR AS gender_code,
        profile:marital_status::VARCHAR AS marital_status,
        profile:religion::VARCHAR AS religion,
        profile:country_of_birth_code::VARCHAR AS country_of_birth_code,
        profile:language_spoken_at_home_code::VARCHAR
            AS language_spoken_at_home_code,
        profile:client_email_comments::VARCHAR AS client_email_comments,
        profile:email::VARCHAR AS email, -- noqa: RF04
        profile:client_phone_comments::VARCHAR AS client_phone_comments,
        profile:phone_main::VARCHAR AS phone_main,
        profile:phone_personal::VARCHAR AS phone_personal,
        profile:phone_other::VARCHAR AS phone_other,
        profile:fax::VARCHAR AS fax,
        profile:pension_status::VARCHAR AS pension_status,
        profile:interpreter_required::VARCHAR AS interpreter_required,
        profile:address_suite::VARCHAR AS address_suite,
        profile:address::VARCHAR AS address,
        profile:city::VARCHAR AS city,
        profile:state::VARCHAR AS state,
        profile:zip::VARCHAR AS zip,
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
        profile:billing_post_code::VARCHAR AS billing_post_code,
        profile:billing_country::VARCHAR AS billing_country,
        profile:ech_retirement_living_resident::VARCHAR
            AS ech_retirement_living_resident,
        profile:hcp_approval_date::DATE AS hcp_approval_date,
        profile:ambulance_card_number::VARCHAR AS ambulance_card_number,
        profile:carer_contact_name_primary_only::VARCHAR
            AS carer_contact_name_primary_only,
        profile:account_contact_name::VARCHAR AS account_contact_name,
        profile:sms_contact_name::VARCHAR AS sms_contact_name,
        profile:sms_contact_number::VARCHAR AS sms_contact_number,
        profile:client_custom_stmt_delivery::VARCHAR
            AS client_custom_stmt_delivery,
        profile:time_of_death::VARCHAR AS time_of_death,
        profile:disability_code::VARCHAR AS disability_code,
        profile:medicare_number::VARCHAR AS medicare_number,
        profile:medicare_card_no::VARCHAR AS medicare_card_no,
        profile:medicare_reference::VARCHAR AS medicare_reference,
        profile:medicare_card_expiry::VARCHAR AS medicare_card_expiry,
        profile:ech_site_location_name::VARCHAR AS ech_site_location_name,
        profile:health_care_provider_number_ea_clinic::VARCHAR
            AS health_care_provider_number_ea_clinic,
        profile:health_care_provider_number_hb_clinic::VARCHAR
            AS health_care_provider_number_hb_clinic,
        profile:health_care_provider_number_vh_clinic::VARCHAR
            AS health_care_provider_number_vh_clinic,
        profile:ech_assisted_living_resident::VARCHAR
            AS ech_assisted_living_resident,
        profile:hcp_client_discharged_to_another_provider::VARCHAR
            AS hcp_client_discharged_to_another_provider,
        profile:client_ndis_number::VARCHAR AS client_ndis_number,
        profile:consent_to_record_lgbtiqa_status::VARCHAR
            AS consent_to_record_lgbtiqa_status,
        profile:consent_for_future_contacts::VARCHAR
            AS consent_for_future_contacts,
        profile:fastcard::VARCHAR AS fastcard,
        profile:dva_ref_prov_num::VARCHAR AS dva_ref_prov_num,
        profile:dva_card_number::VARCHAR AS dva_card_number,
        profile:dva_card_type::VARCHAR AS dva_card_type,
        profile:dva_ref_date::VARCHAR AS dva_ref_date,
        profile:dva_entry_date::VARCHAR AS dva_entry_date,
        profile:centrelink_reference_number::VARCHAR
            AS centrelink_reference_number,
        profile:private_health_insurance::VARCHAR AS private_health_insurance,
        profile:is_billing_contact::VARCHAR AS is_billing_contact,
        profile:accommodation_type_code::VARCHAR AS accommodation_type_code,
        profile:d365_debtor_id::VARCHAR AS d_365_debtor_id,
        profile:household_composition_code::VARCHAR
            AS household_composition_code,
        profile:is_a_carer::VARCHAR AS is_a_carer,
        profile:tags_v2::VARCHAR AS tags_v_2,
        profile:date_of_death::VARCHAR AS date_of_death,
        profile:consent_to_share_information::VARCHAR
            AS consent_to_share_information,
        profile:death_location_category::VARCHAR AS death_location_category,
        profile:hcp_client_transfer_from_another_provider::VARCHAR
            AS hcp_client_transfer_from_another_provider,
        profile:has_carer::VARCHAR AS has_carer,
        profile:carer_residency::VARCHAR AS carer_residency,
        profile:remarks::VARCHAR AS remarks,
        profile:client_is_tpi::VARCHAR AS client_is_tpi,
        profile:health_care_provider_number_cg_clinic::VARCHAR
            AS health_care_provider_number_cg_clinic,
        profile:health_care_provider_number_mv_clinic::VARCHAR
            AS health_care_provider_number_mv_clinic,
        profile:scheduling_preference::VARCHAR AS scheduling_preference,
        profile:scheduling_preferences::VARCHAR AS scheduling_preferences,
        profile:start_date::VARCHAR AS start_date,
        admission_date,
        discharge_date,
        client_brn,
        preferred_language,
        visit_count,
        has_adls
    FROM client
)

SELECT *
FROM flatten_cols
