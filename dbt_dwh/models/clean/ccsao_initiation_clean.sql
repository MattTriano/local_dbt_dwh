with source_data as (
    select
        CASE_ID                               as case_id,
        CASE_PARTICIPANT_ID                   as case_participant_id,
        RECEIVED_DATE                         as received_date,
        upper(OFFENSE_CATEGORY)               as offense_category,
        PRIMARY_CHARGE_FLAG                   as primary_charge_flag,
        CHARGE_ID                             as charge_id,
        CHARGE_VERSION_ID                     as charge_version_id,
        upper(CHARGE_OFFENSE_TITLE)           as charge_offense_title,
        CHARGE_COUNT                          as charge_count,
        upper(CHAPTER)                        as chapter,
        coalesce(
            upper(ACT), split_part(upper(CHAPTER), '-', 2)
        )                                     as act,
        coalesce(
            replace(upper(SECTION), ' ILCS ', '-'), split_part(upper(CHAPTER), '-', 3)
        )                                     as section,
        upper(CLASS)                          as class,
        upper(AOIC)                           as aoic,
        upper(EVENT)                          as event,
        EVENT_DATE                            as event_date,
        FINDING_NO_PROBABLE_CAUSE             as finding_no_probable_cause,
        ARRAIGNMENT_DATE                      as arraignment_date,
        BOND_DATE_INITIAL                     as bond_date_initial,
        BOND_DATE_CURRENT                     as bond_date_current,
        upper(BOND_TYPE_INITIAL)              as bond_type_initial,
        upper(BOND_TYPE_CURRENT)                     as bond_type_current,
        BOND_AMOUNT_INITIAL                   as bond_amount_initial,
        BOND_AMOUNT_CURRENT                   as bond_amount_current,
        BOND_ELECTRONIC_MONITOR_FLAG_INITIAL  as bond_electronic_monitor_flag_initial,
        BOND_ELECTROINIC_MONITOR_FLAG_CURRENT as bond_electroinic_monitor_flag_current,
        AGE_AT_INCIDENT                       as age_at_incident,
        RACE                                  as race,
        case
            when upper(GENDER) like '%UNKNOWN%' then 'UNKNOWN'
            else upper(GENDER)
        end                                   as gender,
        upper(INCIDENT_CITY)                  as incident_city,
        INCIDENT_BEGIN_DATE                   as incident_begin_date,
        INCIDENT_END_DATE                     as incident_end_date,
        regexp_replace(
            upper(LAW_ENFORCEMENT_AGENCY), 'POLICE DEPARTMENT', 'PD', 'g'
        )                                     as law_enforcement_agency,
        upper(LAW_ENFORCEMENT_UNIT)           as law_enforcement_unit,
        ARREST_DATE                           as arrest_date,
        FELONY_REVIEW_DATE                    as felony_review_date,
        case
            when upper(FELONY_REVIEW_RESULT) like '%APPROVED%' then 'APPROVED'
            when upper(FELONY_REVIEW_RESULT) like '%DISREGARD%' then 'REJECTED'
            else upper(FELONY_REVIEW_RESULT)  
        END                                   as felony_review_result,
        upper(UPDATED_OFFENSE_CATEGORY)       as updated_offense_category
    from {{ ref('ccsao_initiation_raw') }}
)

select *
from source_data
