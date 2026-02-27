"""Client application classification mappings and icon generation.

Single source of truth for mapping raw client application strings to display names.
Used by both the SQL stored procedure (via lookup table) and the app's auto-setup.

Each tuple: (ilike_pattern, source_field, display_name)
  - source_field: "application" or "client_app_id"
  - Patterns are matched in order; first match wins.
"""

import base64
import hashlib
from functools import lru_cache

CLIENT_MAPPINGS = [
    # source_field: client_app_id
    ("%snowpark%", "client_app_id", "Snowpark"),
    # source_field: application
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
    ("%DATABRICKS%", "application", "Databricks/Spark"),
    ("%dbatch%", "application", "Databricks/Spark"),
    ("%SPARK%", "application", "Databricks/Spark"),
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
    ("%thoughspot%", "application", "ThoughtSpot"),
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
    ("%python%", "application", "Python"),
    # source_field: client_app_id (fallback patterns when application IS NULL)
    ("%SNOWFLAKE%", "client_app_id", "Snowflake Web"),
    ("%JDBC%", "client_app_id", "JDBC"),
    ("%javascript%", "client_app_id", "Javascript"),
]

# Short abbreviations for client icon badges.
# Keys that don't appear here get an auto-generated abbreviation.
CLIENT_ICON_ABBREVS: dict[str, str] = {
    "Airflow": "Af",
    "Alteryx": "Ax",
    "Apache Maven Surefire": "Mv",
    "ArcGIS": "AG",
    "Astronomer": "As",
    "Azure App Service/WebJobs": "WJ",
    "Azure Data Factory": "AD",
    "Boomi": "Bm",
    "Business Objects": "BO",
    "CData": "CD",
    "Cirrus CI": "Ci",
    "Coalesce": "Co",
    "Cognos": "Cg",
    "COSMOS": "CS",
    "Databricks/Spark": "Db",
    "Dataiku": "Dk",
    "DBeaver": "DB",
    "Diffchecker": "Dc",
    "Domo": "Do",
    "DTLK": "DT",
    "Excel": "Ex",
    "Fads": "Fd",
    "Fivetran": "Ft",
    "FlySpeed SQL": "FS",
    "Grafana": "Gr",
    "Hyperion": "Hy",
    "IBM DataStage": "DS",
    "Informatica Cloud": "Ic",
    "IntelliJ": "IJ",
    "Jarvis": "Jv",
    "Javascript": "JS",
    "JDBC": "JD",
    "Jenkins": "Jk",
    "Kafka": "Kf",
    "Laserfiche": "Lf",
    "Microsoft Access": "MA",
    "Microsoft IIS": "II",
    "Microsoft Office": "MO",
    "MicroStrategy": "MS",
    "NEXIS": "Nx",
    "NICE": "Nc",
    "Nimbus": "Nb",
    "Palantir": "Pl",
    "Perl": "Pr",
    "Power BI": "PB",
    "PowerShell": "PS",
    "Presto": "Pt",
    "Python": "Py",
    "Qlik Replicate": "Qk",
    "RAPPID": "Rp",
    "RStudio": "RS",
    "Salesforce": "Sf",
    "SAS": "SA",
    "Sigma": "Sg",
    "SnowCLI": "SC",
    "Snowflake Web": "Sn",
    "Snowpark": "Sp",
    "SQL Server": "SQ",
    "SSIS": "SS",
    "SSRS/PBIRS": "SR",
    "Starburst": "Sb",
    "Tableau": "Tb",
    "Talend": "Tl",
    "Teradata": "Td",
    "ThoughtSpot": "TS",
    "Tibco Spotfire": "Tc",
    "Toad": "To",
    "Tomcat": "Tm",
    "UiPath": "UP",
    "VSCode": "VS",
    "WhereScape": "WS",
    "AdvancedQueryTool": "AQ",
    "ADBC-Go": "Go",
    "install4j": "i4",
}


def _abbreviation(name: str) -> str:
    """Return a 1-2 letter abbreviation for a client display name."""
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
    """Deterministic hue (0-360) from a client name."""
    h = int(hashlib.md5(name.encode()).hexdigest()[:8], 16)
    return h % 360


@lru_cache(maxsize=256)
def generate_client_icon_uri(name: str) -> str:
    """Generate a base64 SVG data URI icon for a client application.

    Returns a colored circle with a 1-2 letter abbreviation.
    Colors are deterministic per client name.
    """
    abbrev = _abbreviation(name)
    hue = _name_to_hue(name)
    # Use moderate saturation and lightness for readability in both themes
    bg_color = f"hsl({hue}, 55%, 50%)"
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="128" height="128" viewBox="0 0 128 128">'
        f'<circle cx="64" cy="64" r="60" fill="{bg_color}" stroke="white" stroke-width="3"/>'
        f'<text x="64" y="64" text-anchor="middle" dominant-baseline="central" '
        f'font-family="Lato, Arial, sans-serif" font-weight="700" font-size="48" fill="white">'
        f'{abbrev}</text></svg>'
    )
    encoded = base64.b64encode(svg.encode()).decode()
    return f"data:image/svg+xml;base64,{encoded}"
