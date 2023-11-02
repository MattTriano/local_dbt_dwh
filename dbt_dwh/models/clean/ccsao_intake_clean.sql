with source_data as (
    select
        CASE_ID                 AS case_id,
        CASE_PARTICIPANT_ID     AS case_participant_id,
        RECEIVED_DATE           AS received_date,
        OFFENSE_CATEGORY        AS offense_category,
        PARTICIPANT_STATUS      AS participant_status,
        AGE_AT_INCIDENT         AS age_at_incident,
        RACE                    AS race,
        GENDER                  AS gender,
        INCIDENT_CITY           AS incident_city,
        INCIDENT_BEGIN_DATE     AS incident_begin_date,
        INCIDENT_END_DATE       AS incident_end_date,
        LAW_ENFORCEMENT_AGENCY  AS law_enforcement_agency,
        LAW_ENFORCEMENT_UNIT    AS law_enforcement_unit,
        ARREST_DATE             AS arrest_date,
        {{ bring_extreme_dates_into_pd_range('FELONY_REVIEW_DATE', 'felony_review_date') }},
        FELONY_REVIEW_RESULT    AS felony_review_result,
        UPDATE_OFFENSE_CATEGORY AS update_offense_category
    from {{ ref('ccsao_intake_raw') }}
)

select *
from source_data
