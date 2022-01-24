*** Settings ***
Documentation       Inhuman Insurance, Inc. Artificial Intelligence System robot.
...                 Produces traffic data work items.
...                 Author manjunath kannur
Library             RPA.HTTP
Library             RPA.JSON
Library             RPA.Tables
Library             Collections
Resource            shared.robot

*** Variables ***
${json_link}            https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json
${trafic_json}          ${OUTPUT_DIR}${/}output.json
${COUNTRY_KEY}=    SpatialDim
${GENDER_KEY}=    Dim1
${RATE_KEY}=      NumericValue
${YEAR_KEY}=      TimeDim
${max_rate}=        ${5.0}
${both_genders}=      BTSX
    

*** Tasks ***
Produce traffic data work items
    Download traffic data
    ${traffic_data}=        Load traffic data as table  
    Write Table To Csv      ${traffic_data}    test.csv
    ${filtered_data}        Filter and sort traffic data        ${traffic_data}
    ${get_data}             Get latest data by country          ${filtered_data}
    ${payloads}=    Create work item payloads    ${get_data}
    Save work item payloads    ${payloads}

*** Keywords ***
Download traffic data
    Download
    ...    ${json_link}
    ...    ${trafic_json}
    ...    overwrite=True


*** Keywords ***
Load traffic data as table
    ${json_data}=    Load JSON from file    ${OUTPUT_DIR}${/}output.json
    ${table}=    Create Table    ${json_data}[value]
    [Return]    ${table}

*** Keywords ***
Filter and sort traffic data
    [Arguments]     ${my_table}
    Filter Table By Column      ${my_table}         ${RATE_KEY}    <    ${max_rate}
    Filter Table By Column      ${my_table}         ${GENDER_KEY}    ==    ${both_genders}
    Sort Table By Column        ${my_table}         ${YEAR_KEY}    False
    [Return]                    ${my_table}

*** Keywords ***
Get latest data by country
    [Arguments]         ${get_table}
    ${country_key}=    Set Variable    SpatialDim
    ${get_table}=    Group Table By Column    ${get_table}    ${country_key}
    ${country_data}=    Create List
    FOR    ${group}    IN    @{get_table}
        ${first_row}=    Pop Table Row    ${group}
        Append To List    ${country_data}    ${first_row}
    END
    [Return]    ${country_data}

*** Keywords ***
Create work item payloads
    [Arguments]    ${traffic_data}
    ${pay_loads}=    Create List
    FOR    ${row}    IN    @{traffic_data}
        ${pay_load}=
        ...    Create Dictionary
        ...    country=${row}[${COUNTRY_KEY}]
        ...    year=${row}[${YEAR_KEY}]
        ...    rate=${row}[${RATE_KEY}]
        Append To List    ${pay_loads}    ${pay_load}
    END
    [Return]    ${pay_loads}

*** Keywords ***
Save work item payloads
    [Arguments]    ${payloads}
    FOR    ${payload}    IN    @{payloads}
        Save work item payload    ${payload}
    END

*** Keywords ***
Save work item payload
    [Arguments]    ${payload}
    Create Output Work Item
    Set Work Item Variable    traffic_data    ${payload}
    Save Work Item