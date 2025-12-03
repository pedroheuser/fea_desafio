with source as (

    select * from {{ source('adventure_works', 'person_person') }}

)

, renamed as (

    select
        cast(businessentityid as int) as person_id
        , persontype as person_type
        , cast(namestyle as boolean) as name_style
        , title as title
        , firstname as first_name
        , middlename as middle_name
        , lastname as last_name
        , suffix as suffix
        , cast(emailpromotion as int) as email_promotion
        , additionalcontactinfo as additional_contact_info
        , demographics as demographics
        , rowguid as row_guid
        , cast(modifieddate as timestamp) as modified_date
    from source

)

select * from renamed

