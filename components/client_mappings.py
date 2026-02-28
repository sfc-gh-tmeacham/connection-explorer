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
    # Ecosystem partners (from docs.snowflake.com/en/user-guide/ecosystem-all)
    ("%airbyte%", "application", "Airbyte"),
    ("%looker%", "application", "Looker"),
    ("%datastudio%", "application", "Google Data Studio"),
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
    # source_field: client_app_id (fallback patterns when application IS NULL)
    ("%SNOWFLAKE%", "client_app_id", "Snowflake Web"),
    ("%JDBC%", "client_app_id", "JDBC"),
    ("%javascript%", "client_app_id", "Javascript"),
]

# Short abbreviations for client icon badges.
# Keys that don't appear here get an auto-generated abbreviation.
CLIENT_ICON_ABBREVS: dict[str, str] = {
    "ADBC-Go": "Go",
    "AdvancedQueryTool": "AQ",
    "Airbyte": "Ab",
    "Airflow": "Af",
    "Alteryx": "Ax",
    "Apache Maven Surefire": "Mv",
    "Apache Superset": "Su",
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
    "Apache Spark": "Sk",
    "Dagster": "Dg",
    "Databricks": "Db",
    "Datadog": "Dd",
    "Dataiku": "Dk",
    "DBeaver": "DB",
    "dbt": "dt",
    "Diffchecker": "Dc",
    "Domo": "Do",
    "DTLK": "DT",
    "Excel": "Ex",
    "Fads": "Fd",
    "Fivetran": "Ft",
    "FlySpeed SQL": "FS",
    "Google Data Studio": "GD",
    "Grafana": "Gr",
    "Hyperion": "Hy",
    "IBM DataStage": "DS",
    "Informatica Cloud": "Ic",
    "install4j": "i4",
    "IntelliJ": "IJ",
    "Jarvis": "Jv",
    "Javascript": "JS",
    "JDBC": "JD",
    "Jenkins": "Jk",
    "Kafka": "Kf",
    "KNIME": "Kn",
    "Laserfiche": "Lf",
    "Looker": "Lk",
    "Matillion": "Mt",
    "Metabase": "Mb",
    "Microsoft Access": "MA",
    "Microsoft IIS": "II",
    "Microsoft Office": "MO",
    "MicroStrategy": "MS",
    "MuleSoft": "Mu",
    "New Relic": "NR",
    "NEXIS": "Nx",
    "NICE": "Nc",
    "Nimbus": "Nb",
    "PagerDuty": "PD",
    "Palantir": "Pl",
    "Perl": "Pr",
    "Power BI": "PB",
    "PowerShell": "PS",
    "Prefect": "Pf",
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
    "Splunk": "Sl",
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
}


ICONS_DIR = Path(__file__).resolve().parent.parent / "static" / "client-icons"

# Mapping from display name to SVG filename in static/client-icons/.
# Tools listed here get real brand logos; all others fall back to letter circles.
CLIENT_ICON_FILES: dict[str, str] = {
    "ADBC-Go": "go.svg",
    "Airbyte": "airbyte.svg",
    "Airflow": "airflow.svg",
    "Alteryx": "alteryx.svg",
    "Apache Maven Surefire": "apachemaven.svg",
    "Apache Spark": "apachespark.svg",
    "Apache Superset": "apachesuperset.svg",
    "ArcGIS": "arcgis.svg",
    "Astronomer": "astronomer.svg",
    "Azure App Service/WebJobs": "microsoftazure.svg",
    "Azure Data Factory": "azuredatafactory.svg",
    "Boomi": "sap.svg",
    "Business Objects": "sap.svg",
    "Cirrus CI": "cirrusci.svg",
    "Cognos": "ibm.svg",
    "Dagster": "prefect.svg",
    "Databricks": "databricks.svg",
    "Datadog": "datadog.svg",
    "Dataiku": "dataiku.svg",
    "DBeaver": "dbeaver.svg",
    "dbt": "dbt.svg",
    "Excel": "excel.svg",
    "Fivetran": "fivetran.svg",
    "Google Data Studio": "googledatastudio.svg",
    "Grafana": "grafana.svg",
    "Hyperion": "oracle.svg",
    "IBM DataStage": "ibm.svg",
    "Informatica Cloud": "informatica.svg",
    "IntelliJ": "intellijidea.svg",
    "Javascript": "javascript.svg",
    "JDBC": "openjdk.svg",
    "Jenkins": "jenkins.svg",
    "Kafka": "apachekafka.svg",
    "KNIME": "knime.svg",
    "Looker": "looker.svg",
    "Matillion": "matillion.svg",
    "Metabase": "metabase.svg",
    "Microsoft Access": "microsoftaccess.svg",
    "Microsoft IIS": "microsoftazure.svg",
    "Microsoft Office": "microsoftoffice.svg",
    "MicroStrategy": "microstrategy.svg",
    "MuleSoft": "mulesoft.svg",
    "New Relic": "newrelic.svg",
    "PagerDuty": "pagerduty.svg",
    "Palantir": "palantir.svg",
    "Perl": "perl.svg",
    "Power BI": "powerbi.svg",
    "PowerShell": "powershell.svg",
    "Prefect": "prefect.svg",
    "Presto": "presto.svg",
    "Python": "python.svg",
    "Qlik Replicate": "qlik.svg",
    "RStudio": "r.svg",
    "Salesforce": "salesforce.svg",
    "SAS": "sas.svg",
    "Snowflake Web": "snowflake.svg",
    "SnowCLI": "snowflake.svg",
    "Snowpark": "snowflake.svg",
    "Splunk": "splunk.svg",
    "SQL Server": "microsoftsqlserver.svg",
    "SSIS": "microsoftsqlserver.svg",
    "SSRS/PBIRS": "microsoftsqlserver.svg",
    "Starburst": "starburst.svg",
    "Tableau": "tableau.svg",
    "Talend": "talend.svg",
    "Teradata": "teradata.svg",
    "Tibco Spotfire": "tibco.svg",
    "Tomcat": "apachetomcat.svg",
    "Toad": "oracle.svg",
    "UiPath": "uipath.svg",
    "VSCode": "vscode.svg",
}

_PATH_RE = re.compile(r'd="([^"]+)"')
_VIEWBOX_RE = re.compile(r'viewBox="([^"]+)"')


@lru_cache(maxsize=64)
def _load_svg_paths(filename: str) -> tuple[list[str], float] | None:
    """Read an SVG file and extract all path data strings plus the viewBox size.

    Returns (paths, viewbox_size) where viewbox_size is the max of width/height
    from the viewBox attribute, or None if the file can't be read.
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

    Uses a real brand SVG icon when available (loaded from static/client-icons/),
    falling back to a colored circle with a 1-2 letter abbreviation.
    Colors are deterministic per client name.
    """
    hue = _name_to_hue(name)
    bg_color = f"hsl({hue}, 55%, 50%)"

    # Try to load a real brand icon
    icon_file = CLIENT_ICON_FILES.get(name)
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
            f'font-family="Lato, Arial, sans-serif" font-weight="700" font-size="48" fill="white">'
            f'{abbrev}</text></svg>'
        )

    encoded = base64.b64encode(svg.encode()).decode()
    return f"data:image/svg+xml;base64,{encoded}"
