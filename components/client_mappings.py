"""Client application classification mappings and icon generation.

Single source of truth for mapping raw client application strings to display names.
Used by both the SQL stored procedure (via lookup table) and the app's auto-setup.

Each tuple: (ilike_pattern, source_field, display_name)
  - source_field: "application" or "client_app_id"
  - Patterns are matched in order; first match wins.
"""

import base64
import hashlib
import re
from functools import lru_cache
from pathlib import Path

CLIENT_MAPPINGS = [
    # source_field: client_app_id
    ("%snowpark%", "client_app_id", "Snowpark"),
    # source_field: application
    # Specific snowpark/snowflake variants must come before broad %SNOWPARK% / %SNOWFLAKE%
    ("%PythonSnowpark%", "application", "Python Snowpark"),
    ("%SnowparkML%", "application", "SnowparkML"),
    ("%SNOWPARK%", "application", "Snowpark"),
    ("%deployments%", "application", "Kafka"),
    ("%cosmos%", "application", "COSMOS"),
    ("%rappid%", "application", "RAPPID"),
    ("%dtlk%", "application", "DTLK"),
    ("%nice%", "application", "NICE"),
    ("%nexis%", "application", "NEXIS"),
    ("%MASHUP%", "application", "Power BI"),
    ("%POWERBI%", "application", "Power BI"),
    ("%microsoftonprem%", "application", "Power BI"),
    ("%adbc%go%", "application", "ADBC-Go"),
    ("%DTS%", "application", "SSIS"),
    ("%DTEXEC%", "application", "SSIS"),
    ("%datastage%", "application", "IBM DataStage"),
    ("%REPORTSERVER%", "application", "SSRS/PBIRS"),
    ("%MSRS%", "application", "SSRS/PBIRS"),
    ("%REPORTINGSERVICE%", "application", "SSRS/PBIRS"),
    ("%REPORTBUILDER%", "application", "SSRS/PBIRS"),
    ("%VISUALSTUDIO%", "application", "SSRS/PBIRS"),
    ("%SQLSe%", "application", "SQL Server"),
    ("%GRAFANA%", "application", "Grafana"),
    ("%CIRRUS%", "application", "Cirrus CI"),
    ("%TOAD%", "application", "Toad"),
    ("%BOOTSTRAP%", "application", "Tomcat"),
    ("%QLIKREPL%", "application", "Qlik Replicate"),
    ("%rstudio%", "application", "RStudio"),
    ("%MicroStrat%", "application", "MicroStrategy"),
    ("%TABLEAU%", "application", "Tableau"),
    ("%HYPERION%", "application", "Hyperion"),
    ("%softoffice%", "application", "Microsoft Office"),
    ("%msacces%", "application", "Microsoft Access"),
    ("%DATABRICKS%", "application", "Databricks"),
    ("%dbatch%", "application", "Databricks"),
    ("%SPARK%", "application", "Apache Spark"),
    ("%ALTERYX%", "application", "Alteryx"),
    ("%INFA_DI%", "application", "Informatica Cloud"),
    ("%CDATA%", "application", "CData"),
    ("%fivetran%", "application", "Fivetran"),
    ("%tibco%", "application", "Tibco Spotfire"),
    ("%palantir%", "application", "Palantir"),
    ("%PERL%", "application", "Perl"),
    ("%iis%", "application", "Microsoft IIS"),
    ("%inets%", "application", "Microsoft IIS"),
    ("%w3wp%", "application", "Microsoft IIS"),
    ("%BUSINESSOBJECTS%", "application", "Business Objects"),
    ("%bobj%", "application", "Business Objects"),
    ("%DOMO%", "application", "Domo"),
    ("%astronomer%", "application", "Astronomer"),
    ("%DATAFACTORY%", "application", "Azure Data Factory"),
    ("%INTEGRATIONRUNTIME%", "application", "Azure Data Factory"),
    ("%EXCEL%", "application", "Excel"),
    # Specific snowflake variants before broad %SNOWFLAKE% catch-all
    ("%snowflake_dbt%", "application", "dbt"),
    ("%SNOWFLAKE_CLI%", "application", "SnowCLI"),
    ("%SNOWFLAKE%", "application", "Snowflake Web"),
    ("%JARVIS%", "application", "Jarvis"),
    ("%WEBJOBS%", "application", "Azure App Service/WebJobs"),
    ("%JENKINS%", "application", "Jenkins"),
    ("%KAFKA%", "application", "Kafka"),
    ("%airflow%", "application", "Airflow"),
    ("%starburst%", "application", "Starburst"),
    ("%prest%", "application", "Presto"),
    ("%boomi%", "application", "Boomi"),
    ("%SAS%", "application", "SAS"),
    ("%arcgis%", "application", "ArcGIS"),
    ("%dbeave%", "application", "DBeaver"),
    ("%vscode%", "application", "VSCode"),
    ("%teradata%", "application", "Teradata"),
    ("%powershell%", "application", "PowerShell"),
    ("%uipath%", "application", "UiPath"),
    ("%fads%", "application", "Fads"),
    ("%snowcli%", "application", "SnowCLI"),
    ("%intellij%", "application", "IntelliJ"),
    ("%sigma%", "application", "Sigma"),
    ("%talend%", "application", "Talend"),
    ("%thoughtspot%", "application", "ThoughtSpot"),
    ("%install4j%", "application", "install4j"),
    ("%cognos%", "application", "Cognos"),
    ("%nimbus%", "application", "Nimbus"),
    ("%surefire%", "application", "Apache Maven Surefire"),
    ("%dataiku%", "application", "Dataiku"),
    ("%laserfiche%", "application", "Laserfiche"),
    ("%coalesce%", "application", "Coalesce"),
    ("%wherescape%", "application", "WhereScape"),
    ("%salesforce%", "application", "Salesforce"),
    ("%diffcheck%", "application", "Diffchecker"),
    ("%flyspeed%", "application", "FlySpeed SQL"),
    ("%AdvancedQuery%", "application", "AdvancedQueryTool"),
    # Ecosystem partners (from docs.snowflake.com/en/user-guide/ecosystem-all)
    ("%airbyte%", "application", "Airbyte"),
    ("%looker%", "application", "Looker"),
    ("%datastudio%", "application", "Google Data Studio"),
    ("%gcpdatastudio%", "application", "Google Data Studio"),
    ("%metabase%", "application", "Metabase"),
    ("%superset%", "application", "Apache Superset"),
    ("%matillion%", "application", "Matillion"),
    ("%knime%", "application", "KNIME"),
    ("%splunk%", "application", "Splunk"),
    ("%newrelic%", "application", "New Relic"),
    ("%datadog%", "application", "Datadog"),
    ("%pagerduty%", "application", "PagerDuty"),
    ("%mulesoft%", "application", "MuleSoft"),
    ("%prefect%", "application", "Prefect"),
    ("%dagster%", "application", "Dagster"),
    ("%dbt%", "application", "dbt"),
    ("%python%", "application", "Python"),
    # BI / Analytics (from SE_PARSE_SESSION_TOOL UDF)
    ("%periscope%", "application", "Periscope"),
    ("%qvconnect%", "application", "QlikView"),
    ("%QlikCustom%", "application", "Qlik Sense"),
    ("%QlikDataTransfer%", "application", "Qlik Sense"),
    ("%QlikSense%", "application", "Qlik Sense"),
    ("%qvodbc%", "application", "Qlik Sense"),
    ("%qlikqcs%", "application", "Qlik Sense SaaS"),
    ("%spotfire%", "application", "Tibco Spotfire"),
    ("%mode%analytics%", "application", "Mode"),
    ("%zoomdata%", "application", "Zoom Data"),
    ("%birst%", "application", "Birst"),
    ("%numeracy%", "application", "Numeracy"),
    ("%quicksight%", "application", "Quicksight"),
    ("%chartio%", "application", "Chartio"),
    ("%cluvio%", "application", "Cluvio"),
    ("%astrato%", "application", "Astrato"),
    ("%GoodData_Platform%", "application", "GoodData"),
    ("%GoodData_GoodDataCN%", "application", "GoodData"),
    ("%holistics%", "application", "Holistics"),
    ("%Pyramid_%", "application", "Pyramid"),
    ("%MachEye%", "application", "MachEye"),
    ("%tellius%", "application", "Tellius"),
    ("%count.co%", "application", "Count"),
    ("%sisense%", "application", "Sisense"),
    ("%SAP%BusinessObjects%", "application", "SAP BOBJ"),
    ("%atscale%", "application", "AtScale"),
    ("%thoughtspot%", "application", "ThoughtSpot"),
    # Data Science / ML
    ("%dplyr%", "application", "R"),
    ("%Rscriptexe%", "application", "R"),
    ("%Rterm%", "application", "R"),
    ("%datarobot%", "application", "DataRobot"),
    ("%h2o%", "application", "H2O"),
    ("%streamsets%", "application", "StreamSets"),
    ("%bigsquid%", "application", "BigSquid"),
    ("%big-squid-kraken%", "application", "BigSquid"),
    ("%rasgo%", "application", "Rasgo"),
    ("%rasgoql%", "application", "Rasgo"),
    ("%DominoDataLab%", "application", "Domino Data Lab"),
    ("%sisu_data%", "application", "Sisu"),
    ("%peak_ai%", "application", "Peak AI"),
    ("%modelbit%", "application", "Modelbit"),
    ("%continual%", "application", "Continual"),
    ("%SnorkelAI%", "application", "Snorkel AI"),
    ("%tecton-ai%", "application", "Tecton"),
    ("%AmazonSageMakerDataWrangler%", "application", "Amazon SageMaker"),
    ("%dask%", "application", "Dask"),
    ("%atoti%", "application", "Atoti"),
    ("%activeviam%", "application", "ActiveViam"),
    # ETL / Data Integration
    ("%segment%", "application", "Segment"),
    ("%stitchdata%", "application", "Stitch"),
    ("%snaplogic%", "application", "SnapLogic"),
    ("%pentaho%", "application", "Pentaho"),
    ("%nifi%", "application", "NiFi"),
    ("%snowplow%", "application", "Snowplow"),
    ("%sqoop%", "application", "Apache Sqoop"),
    ("%apache.storm%", "application", "Alooma"),
    ("%workato%", "application", "Workato"),
    ("%hvr%", "application", "HVR"),
    ("%etleap%", "application", "Etleap"),
    ("%awsglue%", "application", "AWS Glue"),
    ("%gcpdataflow%", "application", "GCP Dataflow"),
    ("%gcpdatafusion%", "application", "GCP Data Fusion"),
    ("%rivery%", "application", "Rivery"),
    ("%striim%", "application", "Striim"),
    ("%diyotta%", "application", "Diyotta"),
    ("%Xplenty%", "application", "Xplenty"),
    ("%celigo%", "application", "Celigo"),
    ("%infoworks%", "application", "Infoworks"),
    ("%SafeSoftwareFME%", "application", "SafeSoftware FME"),
    ("%WANdisco%", "application", "WANdisco"),
    ("%Gluent%", "application", "Gluent"),
    ("%hevodata%", "application", "HevoData"),
    ("%rudderstack%", "application", "RudderStack"),
    ("%nexla%", "application", "Nexla"),
    ("%agiledataengine%", "application", "Agile Data Engine"),
    ("%ADVERITY%", "application", "Adverity"),
    ("%Supermetrics%", "application", "Supermetrics"),
    ("%hightouch%", "application", "Hightouch"),
    ("%PreciselyConnect%", "application", "Precisely"),
    ("%PreciselyConnectCDC%", "application", "Precisely"),
    ("%PreciselyProfiling%", "application", "Precisely"),
    ("%Omnata%", "application", "Omnata"),
    ("%flywheel%", "application", "Flywheel"),
    ("%sarasanalytics_daton%", "application", "Daton"),
    ("%kleene%", "application", "Kleene"),
    ("%CapStorm%", "application", "CapStorm"),
    ("%CData_JDBC%", "application", "CData"),
    ("%CData_Software%", "application", "CData"),
    ("%tableau prep%", "application", "Tableau Prep"),
    ("%AzureDataFactory%", "application", "Azure Data Factory"),
    # Data Quality / Governance
    ("%alation%", "application", "Alation"),
    ("%Collibra%", "application", "Collibra"),
    ("%owl-webapp%", "application", "Collibra DQ"),
    ("%IMMUTA%", "application", "Immuta"),
    ("%atlan%", "application", "Atlan"),
    ("%Monte Carlo%", "application", "Monte Carlo"),
    ("%metaplane%", "application", "Metaplane"),
    ("%SELECTSTAR%", "application", "Select Star"),
    ("%Anomalo%", "application", "Anomalo"),
    ("%DQLabs%", "application", "DQLabs"),
    ("%BigID%", "application", "BigID"),
    ("%castor%", "application", "Castor"),
    ("%octopaiclient%", "application", "Octopai"),
    ("%ataccama%", "application", "Ataccama"),
    ("%OvalEdge%", "application", "OvalEdge"),
    ("%acryl_datahub%", "application", "Acryl DataHub"),
    ("%datagalaxy%", "application", "DataGalaxy"),
    ("%manta%", "application", "Manta"),
    ("%OneTrust%", "application", "OneTrust"),
    ("%privacera%", "application", "Privacera"),
    ("%securiti%", "application", "Securiti"),
    ("%TrustLogix%", "application", "TrustLogix"),
    ("%bigeye%", "application", "Bigeye"),
    ("%validatar%", "application", "Validatar"),
    ("%tamr%", "application", "Tamr"),
    ("%datadotworld%", "application", "Data.world"),
    ("%altr_solutions%", "application", "ALTR"),
    # Security / Observability
    ("%lacework%", "application", "Lacework"),
    ("%Panther%", "application", "Panther"),
    ("%securonix%", "application", "Securonix"),
    ("%Hunters_OpenXDR%", "application", "Hunters"),
    ("%anodot%", "application", "Anodot"),
    # Developer / Other tools
    ("%cortex_code%", "application", "Cortex Code"),
    ("%jupyter%", "application", "Jupyter"),
    ("%spcs%", "application", "SPCS"),
    ("Go", "application", "Go"),
    ("%ruby%", "application", "Ruby"),
    ("%SnowSQL%", "application", "SnowSQL"),
    ("%liquibase%", "application", "Liquibase"),
    ("%schemachange%", "application", "Schemachange"),
    ("%streamlit%", "application", "Streamlit"),
    ("%hex_technologies%", "application", "Hex"),
    ("%hex_python%", "application", "Hex"),
    ("%hex_sql%", "application", "Hex"),
    ("%Deepnote%", "application", "Deepnote"),
    ("%Devart%", "application", "Devart"),
    ("%hackolade%", "application", "Hackolade"),
    ("%sqldbm%", "application", "sqlDBM"),
    ("%seekwell%", "application", "SeekWell"),
    ("%trifacta%", "application", "Trifacta"),
    ("%dataform%", "application", "Dataform"),
    ("%Datameer%", "application", "Datameer"),
    ("%jmeter%", "application", "JMeter"),
    ("%OracleIdeLauncher%", "application", "Oracle SQL Developer"),
    ("%Compose%", "application", "Compose"),
    ("%AttunityCompose%", "application", "Compose"),
    ("%AttunityReplicate%", "application", "Qlik Replicate"),
    ("%repctl%", "application", "Qlik Replicate"),
    ("%coiled_cloud%", "application", "Coiled Cloud"),
    ("%MessageGears%", "application", "MessageGears"),
    ("%mobilize.net%", "application", "Mobilize.net"),
    ("%celonis%", "application", "Celonis"),
    ("%Innovaccer%", "application", "Innovaccer"),
    ("%Waystar%", "application", "Waystar"),
    ("%ZirMed%", "application", "Waystar"),
    ("%ElysiumAnalytics%", "application", "Elysium Analytics"),
    ("%icedq%", "application", "IceDQ"),
    ("%sparkflows%", "application", "Sparkflows"),
    ("%CARTO%", "application", "CartoDB"),
    ("%denodo%", "application", "Denodo"),
    ("%datavaultbuilder%", "application", "Data Vault Builder"),
    ("%dataops%", "application", "DataOps"),
    ("%erwindm%", "application", "Erwin DM"),
    ("%erwindi%", "application", "Erwin DI"),
    ("%amplitude%", "application", "Amplitude"),
    ("%Snowboard%", "application", "Snowboard"),
    ("%ascend.io%", "application", "Ascend"),
    ("%DataJoinery%", "application", "DataJoinery"),
    ("%PipedJobRunner%", "application", "Catalog"),
    ("%datagaps%", "application", "Datagaps"),
    ("%TIBCO_BW%", "application", "Tibco BusinessWorks"),
    ("%TIBCO_Spotfire%", "application", "Tibco Spotfire"),
    ("%modzy%", "application", "Modzy"),
    ("%pramana_shift%", "application", "Pramana Shift"),
    ("%APOSLDG%", "application", "APOS"),
    ("%SalesforceEinstein%", "application", "Salesforce Einstein"),
    ("%Quest_ToadDataPoint%", "application", "Toad Data Point"),
    ("%aquera%", "application", "Aquera"),
    ("%PythonConnector%", "application", "Python"),
    ("%odbcad%", "application", "ODBC"),
    ("%API%", "application", "API"),
    ("%ToscaTestsuite%", "application", "Tricentis Tosca"),
    ("%Heap%", "application", "Heap"),
    ("%Mixpanel%", "application", "Mixpanel"),
    ("%Bizible%", "application", "Bizible"),
    ("%netsuite%", "application", "NetSuite"),
    ("%Zepl%", "application", "Zepl"),
    ("%zeppelin%", "application", "Zeppelin"),
    ("%actable%", "application", "Actable"),
    ("%Microsoft%SQL%Server%", "application", "SQL Server"),
    ("%kloudgen%", "application", "Kloudgen"),
    ("%SAVANT%", "application", "Savant"),
    ("%ibm%information%server%", "application", "IBM Information Server"),
    ("%ibm%spss%", "application", "IBM SPSS"),
    # source_field: client_app_id (fallback patterns when application IS NULL)
    ("%SNOWFLAKE%", "client_app_id", "Snowflake Web"),
    ("%JDBC%", "client_app_id", "JDBC"),
    ("%javascript%", "client_app_id", "Javascript"),
]

# Short abbreviations for client icon badges.
# Keys that don't appear here get an auto-generated abbreviation.
CLIENT_ICON_ABBREVS: dict[str, str] = {
    "Actable": "Ac",
    "ActiveViam": "AV",
    "ADBC-Go": "Go",
    "AdvancedQueryTool": "AQ",
    "Adverity": "Av",
    "Agile Data Engine": "AE",
    "Airbyte": "Ab",
    "Airflow": "Af",
    "Alation": "Al",
    "Alooma": "Am",
    "Alteryx": "Ax",
    "ALTR": "AT",
    "Amazon SageMaker": "SM",
    "Amplitude": "Am",
    "Anomalo": "An",
    "Anodot": "Ad",
    "Apache Maven Surefire": "Mv",
    "Apache Sqoop": "Sq",
    "Apache Spark": "Sk",
    "Apache Superset": "Su",
    "APOS": "AP",
    "Aquera": "Aq",
    "ArcGIS": "AG",
    "Ascend": "Ac",
    "Astronomer": "As",
    "Astrato": "At",
    "Ataccama": "Ac",
    "Atlan": "An",
    "Atoti": "Ao",
    "AtScale": "AS",
    "AWS Glue": "Gl",
    "Azure App Service/WebJobs": "WJ",
    "Azure Data Factory": "AD",
    "Bigeye": "Be",
    "BigID": "BI",
    "BigSquid": "BS",
    "Birst": "Bi",
    "Bizible": "Bz",
    "Boomi": "Bm",
    "Business Objects": "BO",
    "CapStorm": "Ca",
    "CartoDB": "Ct",
    "Castor": "Cs",
    "Catalog": "Cl",
    "CData": "CD",
    "Celigo": "Ce",
    "Celonis": "Cn",
    "Chartio": "Ch",
    "Cirrus CI": "Ci",
    "Cluvio": "Cv",
    "Coalesce": "Co",
    "Cognos": "Cg",
    "Coiled Cloud": "CC",
    "Collibra": "Cb",
    "Collibra DQ": "CQ",
    "Compose": "Cm",
    "Continual": "Ct",
    "COSMOS": "CS",
    "Count": "Cn",
    "Cortex Code": "CX",
    "Dagster": "Dg",
    "Dask": "Dk",
    "Data Vault Builder": "DV",
    "Data.world": "Dw",
    "Databricks": "Db",
    "Datadog": "Dd",
    "Dataform": "Df",
    "DataGalaxy": "DG",
    "Datagaps": "Dp",
    "Dataiku": "Dk",
    "DataJoinery": "DJ",
    "Datameer": "Dm",
    "DataOps": "DO",
    "DataRobot": "DR",
    "Daton": "Dn",
    "DBeaver": "DB",
    "dbt": "dt",
    "Deepnote": "Dn",
    "Denodo": "De",
    "Devart": "Dv",
    "Diffchecker": "Dc",
    "Diyotta": "Dy",
    "Domino Data Lab": "DL",
    "Domo": "Do",
    "DQLabs": "DQ",
    "DTLK": "DT",
    "Elysium Analytics": "EA",
    "Erwin DI": "EI",
    "Erwin DM": "EM",
    "Etleap": "El",
    "Excel": "Ex",
    "Fads": "Fd",
    "Fivetran": "Ft",
    "Flywheel": "Fw",
    "FlySpeed SQL": "FS",
    "GCP Data Fusion": "GF",
    "GCP Dataflow": "GD",
    "Gluent": "Gl",
    "GoodData": "GD",
    "Go": "Go",
    "Google Data Studio": "GS",
    "Grafana": "Gr",
    "H2O": "H2",
    "Hackolade": "Hk",
    "Heap": "Hp",
    "HevoData": "Hv",
    "Hex": "Hx",
    "Hightouch": "Ht",
    "Holistics": "Hl",
    "Hunters": "Hu",
    "HVR": "HV",
    "Hyperion": "Hy",
    "IBM DataStage": "DS",
    "IBM Information Server": "IS",
    "IBM SPSS": "SP",
    "IceDQ": "IQ",
    "Immuta": "Im",
    "Informatica Cloud": "Ic",
    "Infoworks": "Iw",
    "Innovaccer": "In",
    "install4j": "i4",
    "IntelliJ": "IJ",
    "Jarvis": "Jv",
    "Javascript": "JS",
    "JDBC": "JD",
    "Jenkins": "Jk",
    "JMeter": "JM",
    "Jupyter": "Jp",
    "Kafka": "Kf",
    "Kleene": "Kl",
    "Kloudgen": "Kg",
    "KNIME": "Kn",
    "Lacework": "Lw",
    "Laserfiche": "Lf",
    "Liquibase": "Lq",
    "Looker": "Lk",
    "MachEye": "ME",
    "Manta": "Ma",
    "Matillion": "Mt",
    "MessageGears": "MG",
    "Metabase": "Mb",
    "Metaplane": "Mp",
    "Microsoft Access": "MA",
    "Microsoft IIS": "II",
    "Microsoft Office": "MO",
    "MicroStrategy": "MS",
    "Mixpanel": "Mx",
    "Mobilize.net": "Mz",
    "Mode": "Md",
    "Modelbit": "Ml",
    "Modzy": "Mz",
    "Monte Carlo": "MC",
    "MuleSoft": "Mu",
    "NetSuite": "NS",
    "New Relic": "NR",
    "NEXIS": "Nx",
    "Nexla": "Nl",
    "NICE": "Nc",
    "NiFi": "Nf",
    "Nimbus": "Nb",
    "Numeracy": "Nu",
    "ODBC": "OD",
    "Octopai": "Op",
    "Omnata": "Om",
    "OneTrust": "OT",
    "Oracle SQL Developer": "OD",
    "OvalEdge": "OE",
    "PagerDuty": "PD",
    "Palantir": "Pl",
    "Panther": "Pn",
    "Peak AI": "PA",
    "Pentaho": "Ph",
    "Periscope": "Ps",
    "Perl": "Pr",
    "Power BI": "PB",
    "PowerShell": "PS",
    "Pramana Shift": "Pm",
    "Precisely": "Pc",
    "Prefect": "Pf",
    "Presto": "Pt",
    "Privacera": "Pv",
    "Pyramid": "Py",
    "Python": "Py",
    "Python Snowpark": "SP",
    "Qlik Replicate": "Qk",
    "Qlik Sense": "QS",
    "Qlik Sense SaaS": "Qs",
    "QlikView": "QV",
    "Quicksight": "Qs",
    "R": "R",
    "RAPPID": "Rp",
    "Rasgo": "Rg",
    "Rivery": "Rv",
    "RStudio": "RS",
    "Ruby": "Rb",
    "RudderStack": "Rd",
    "SafeSoftware FME": "FM",
    "Salesforce": "Sf",
    "Salesforce Einstein": "SE",
    "SAP BOBJ": "SB",
    "SAS": "SA",
    "Savant": "Sv",
    "Schemachange": "Sc",
    "Securiti": "St",
    "Securonix": "Sx",
    "SeekWell": "Sw",
    "Segment": "Se",
    "Select Star": "SS",
    "Sigma": "Sg",
    "Sisense": "Si",
    "Sisu": "Su",
    "SnapLogic": "SL",
    "SnowCLI": "SC",
    "Snowboard": "Sb",
    "Snowflake Web": "Sn",
    "Snowpark": "Sp",
    "SnowparkML": "ML",
    "Snowplow": "Sw",
    "Snorkel AI": "Sk",
    "SnowSQL": "SQ",
    "SPCS": "SP",
    "Sparkflows": "Sf",
    "Splunk": "Sl",
    "SQL Server": "SQ",
    "sqlDBM": "sD",
    "SSIS": "SS",
    "SSRS/PBIRS": "SR",
    "Starburst": "Sb",
    "Stitch": "St",
    "Streamlit": "SL",
    "StreamSets": "Ss",
    "Striim": "Sr",
    "Supermetrics": "Sm",
    "Tableau": "Tb",
    "Tableau Prep": "TP",
    "Talend": "Tl",
    "Tamr": "Tr",
    "Tecton": "Tn",
    "Tellius": "Te",
    "Teradata": "Td",
    "ThoughtSpot": "TS",
    "Tibco BusinessWorks": "TW",
    "Tibco Spotfire": "Tc",
    "Toad": "To",
    "Toad Data Point": "DP",
    "Tomcat": "Tm",
    "Tricentis Tosca": "TT",
    "Trifacta": "Tf",
    "TrustLogix": "TL",
    "UiPath": "UP",
    "Validatar": "Va",
    "VSCode": "VS",
    "WANdisco": "WD",
    "Waystar": "Wy",
    "WhereScape": "WS",
    "Workato": "Wk",
    "Xplenty": "Xp",
    "Zepl": "Zp",
    "Zeppelin": "Zn",
    "Zoom Data": "ZD",
    "Acryl DataHub": "DH",
    "API": "AP",
}


ICONS_DIR = Path(__file__).resolve().parent.parent / "static" / "client-icons"

# Mapping from display name to SVG filename in static/client-icons/.
# Tools listed here get real brand logos; all others fall back to letter circles.
CLIENT_ICON_FILES: dict[str, str] = {
    "ADBC-Go": "go.svg",
    "Airbyte": "airbyte.svg",
    "Airflow": "airflow.svg",
    "Alteryx": "alteryx.svg",
    "Amplitude": "amplitude.svg",
    "Apache Maven Surefire": "apachemaven.svg",
    "Apache Spark": "apachespark.svg",
    "Apache Sqoop": "apache.svg",
    "Apache Superset": "apachesuperset.svg",
    "ArcGIS": "arcgis.svg",
    "Astronomer": "astronomer.svg",
    "AWS Glue": "awsglue.svg",
    "Azure App Service/WebJobs": "microsoftazure.svg",
    "Azure Data Factory": "azuredatafactory.svg",
    "Boomi": "sap.svg",
    "Business Objects": "sap.svg",
    "CartoDB": "carto.svg",
    "Cirrus CI": "cirrusci.svg",
    "Cognos": "ibm.svg",
    "Cortex Code": "terminal.svg",
    "Dagster": "prefect.svg",
    "Dask": "dask.svg",
    "Databricks": "databricks.svg",
    "Datadog": "datadog.svg",
    "Dataiku": "dataiku.svg",
    "DBeaver": "dbeaver.svg",
    "dbt": "dbt.svg",
    "Deepnote": "deepnote.svg",
    "Excel": "excel.svg",
    "Fivetran": "fivetran.svg",
    "GCP Dataflow": "googledataflow.svg",
    "Google Data Studio": "googledatastudio.svg",
    "Go": "go.svg",
    "Grafana": "grafana.svg",
    "Hyperion": "oracle.svg",
    "IBM DataStage": "ibm.svg",
    "IBM Information Server": "ibm.svg",
    "IBM SPSS": "ibm.svg",
    "Informatica Cloud": "informatica.svg",
    "IntelliJ": "intellijidea.svg",
    "Javascript": "javascript.svg",
    "JDBC": "openjdk.svg",
    "Jenkins": "jenkins.svg",
    "JMeter": "apachejmeter.svg",
    "Jupyter": "jupyter.svg",
    "Kafka": "apachekafka.svg",
    "KNIME": "knime.svg",
    "Liquibase": "liquibase.svg",
    "Looker": "looker.svg",
    "Matillion": "matillion.svg",
    "Metabase": "metabase.svg",
    "Microsoft Access": "microsoftaccess.svg",
    "Microsoft IIS": "microsoftazure.svg",
    "Microsoft Office": "microsoftoffice.svg",
    "MicroStrategy": "microstrategy.svg",
    "MuleSoft": "mulesoft.svg",
    "Mixpanel": "mixpanel.svg",
    "New Relic": "newrelic.svg",
    "NiFi": "apachenifi.svg",
    "Oracle SQL Developer": "oracle.svg",
    "PagerDuty": "pagerduty.svg",
    "Palantir": "palantir.svg",
    "Perl": "perl.svg",
    "Power BI": "powerbi.svg",
    "PowerShell": "powershell.svg",
    "Prefect": "prefect.svg",
    "Presto": "presto.svg",
    "Python": "python.svg",
    "Python Snowpark": "snowflake.svg",
    "Qlik Replicate": "qlik.svg",
    "Qlik Sense": "qlik.svg",
    "Qlik Sense SaaS": "qlik.svg",
    "QlikView": "qlik.svg",
    "Quicksight": "quicksight.svg",
    "R": "r.svg",
    "RStudio": "r.svg",
    "Ruby": "ruby.svg",
    "Salesforce": "salesforce.svg",
    "Salesforce Einstein": "salesforce.svg",
    "SAP BOBJ": "sap.svg",
    "SAS": "sas.svg",
    "Schemachange": "snowflake.svg",
    "Segment": "segment.svg",
    "Sigma": "sigma.svg",
    "Snowboard": "snowflake.svg",
    "Snowflake Web": "snowflake.svg",
    "SnowCLI": "snowflake.svg",
    "Snowpark": "snowflake.svg",
    "SnowparkML": "snowflake.svg",
    "SnowSQL": "snowflake.svg",
    "SPCS": "snowflake.svg",
    "Splunk": "splunk.svg",
    "SQL Server": "microsoftsqlserver.svg",
    "SSIS": "microsoftsqlserver.svg",
    "SSRS/PBIRS": "microsoftsqlserver.svg",
    "Starburst": "starburst.svg",
    "Stitch": "stitch.svg",
    "Streamlit": "streamlit.svg",
    "Tableau": "tableau.svg",
    "Tableau Prep": "tableau.svg",
    "Talend": "talend.svg",
    "Teradata": "teradata.svg",
    "Tibco BusinessWorks": "tibco.svg",
    "Tibco Spotfire": "tibco.svg",
    "Toad": "oracle.svg",
    "Toad Data Point": "oracle.svg",
    "Tomcat": "apachetomcat.svg",
    "UiPath": "uipath.svg",
    "VSCode": "vscode.svg",
}

_PATH_RE = re.compile(r'd="([^"]+)"')
_VIEWBOX_RE = re.compile(r'viewBox="([^"]+)"')


@lru_cache(maxsize=64)
def _load_svg_paths(filename: str) -> tuple[list[str], float] | None:
    """Read an SVG file and extract all ``<path>`` data strings and viewBox size.

    Args:
        filename: SVG filename relative to ``ICONS_DIR``.

    Returns:
        A tuple ``(paths, viewbox_size)`` where *paths* is a list of SVG path
        ``d`` attribute strings and *viewbox_size* is the max of width/height
        from the ``viewBox`` attribute.  Returns ``None`` if the file does not
        exist or contains no paths.
    """
    svg_path = ICONS_DIR / filename
    if not svg_path.exists():
        return None
    text = svg_path.read_text(encoding="utf-8")
    paths = _PATH_RE.findall(text)
    if not paths:
        return None
    # Parse viewBox to get the native coordinate size
    vb_match = _VIEWBOX_RE.search(text)
    if vb_match:
        parts = vb_match.group(1).split()
        vb_size = max(float(parts[2]), float(parts[3]))
    else:
        vb_size = 24.0  # simple-icons default
    return paths, vb_size


def _abbreviation(name: str) -> str:
    """Return a 1-2 letter abbreviation for a client display name.

    Looks up ``CLIENT_ICON_ABBREVS`` first; if not found, auto-generates
    an abbreviation from the first letter plus the next uppercase letter
    or digit.

    Args:
        name: The client display name (e.g. ``"Power BI"``).

    Returns:
        A 1-2 character uppercase string (e.g. ``"PB"``).
    """
    if name in CLIENT_ICON_ABBREVS:
        return CLIENT_ICON_ABBREVS[name]
    # Auto-generate: first letter + first consonant/uppercase after it
    clean = name.replace("/", "").replace(" ", "")
    if len(clean) <= 2:
        return clean.upper()
    abbrev = clean[0].upper()
    for ch in clean[1:]:
        if ch.isupper() or ch.isdigit():
            abbrev += ch
            break
    else:
        abbrev += clean[1].lower()
    return abbrev[:2]


def _name_to_hue(name: str) -> int:
    """Derive a deterministic hue value from a client name.

    Uses an MD5 hash so the same name always maps to the same color.

    Args:
        name: The client display name.

    Returns:
        An integer in the range 0-359 representing an HSL hue.
    """
    h = int(hashlib.md5(name.encode()).hexdigest()[:8], 16)
    return h % 360


@lru_cache(maxsize=256)
def generate_client_icon_uri(name: str) -> str:
    """Generate a base64 SVG data URI icon for a client application.

    Uses a real brand SVG icon when available (loaded from
    ``static/client-icons/``), falling back to a colored circle with a
    1-2 letter abbreviation.  Background colors are deterministic per
    client name via ``_name_to_hue``.

    Args:
        name: The client display name (e.g. ``"Tableau"``).

    Returns:
        A ``data:image/svg+xml;base64,...`` URI string suitable for use
        as a vis.js image node.
    """
    hue = _name_to_hue(name)
    bg_color = f"hsl({hue}, 55%, 50%)"

    # Try to load a real brand icon
    icon_file = CLIENT_ICON_FILES.get(name)
    if not icon_file and name.startswith("Snowflake Web"):
        icon_file = CLIENT_ICON_FILES.get("Snowflake Web")
    svg_data = _load_svg_paths(icon_file) if icon_file else None

    if svg_data:
        paths, vb_size = svg_data
        # Scale the icon to fit inside a 72x72 area centered in 128x128
        icon_area = 72.0
        scale = icon_area / vb_size
        offset = (128.0 - vb_size * scale) / 2.0
        paths_svg = "".join(f'<path d="{p}" fill="white"/>' for p in paths)
        svg = (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="128" height="128" viewBox="0 0 128 128">'
            f'<circle cx="64" cy="64" r="60" fill="{bg_color}" stroke="white" stroke-width="3"/>'
            f'<g transform="translate({offset:.1f},{offset:.1f}) scale({scale:.4f})">'
            f'{paths_svg}'
            f'</g></svg>'
        )
    else:
        # Fallback: colored circle with letter abbreviation
        abbrev = _abbreviation(name)
        svg = (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="128" height="128" viewBox="0 0 128 128">'
            f'<circle cx="64" cy="64" r="60" fill="{bg_color}" stroke="white" stroke-width="3"/>'
            f'<text x="64" y="64" text-anchor="middle" dominant-baseline="central" '
            f'font-family="Arial, -apple-system, BlinkMacSystemFont, sans-serif" font-weight="700" font-size="48" fill="white">'
            f'{abbrev}</text></svg>'
        )

    encoded = base64.b64encode(svg.encode()).decode()
    return f"data:image/svg+xml;base64,{encoded}"
