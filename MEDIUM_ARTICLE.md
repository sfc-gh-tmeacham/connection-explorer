# Seeing the Invisible: How I Built a Network Map for My Snowflake Data Estate

*Understanding who accesses what in your data lake shouldn't require a PhD in data archaeology*

---

## The Problem That Kept Me Up at Night

Picture this: You're managing a Snowflake data estate with dozens of databases, multiple warehouses, and hundreds of users accessing data through Tableau, Power BI, Python notebooks, Databricks, and countless other tools. Someone asks you a simple question: "Which databases does our marketing team's Tableau dashboard actually use?"

You stare blankly. You *could* dig through logs, write complex SQL queries against `account_usage` views, export CSVs, and spend hours piecing together the puzzle. Or you could just... not know.

This was my reality. And I knew there had to be a better way.

## The Revelation: Snowflake Already Knows Everything

Here's what's amazing about Snowflake: it's already meticulously tracking every single interaction with your data. Through **Snowflake Horizon Catalog** and the `account_usage` schema, Snowflake records:

- Which databases were accessed
- Which warehouses executed the queries  
- Which client applications made the requests
- When it happened
- Who did it

The data is *right there*. It's just buried in tables with millions of rows, requiring complex joins and deep SQL knowledge to extract meaningful insights.

But what if you could *see* it? What if your entire data access pattern was visualized as an interactive network graph, updating itself automatically, requiring zero maintenance?

That's why I built **Data Lake Explorer**.

## What It Does (And Why You'll Love It)

Data Lake Explorer is a Streamlit application that transforms Snowflake's access logs into a beautiful, interactive network visualization. Here's what you get:

### 🌐 Interactive Network Graph
Watch your data flow come to life. Databases and warehouses appear as nodes, connected by the applications that bridge them. Hover over any connection to see exactly which client (Tableau? Python? JDBC?) is accessing which database through which warehouse.

### 📊 Instant Analytics
Three interactive bar charts show you:
- **Top client applications** by access count
- **Most accessed databases**  
- **Busiest warehouses**

### 🔍 Smart Client Detection
The app automatically recognizes **over 50 different application types**—from BI tools like Tableau and Power BI to ETL platforms like Databricks and Airflow, to development tools like Python, JDBC, and VS Code. No configuration required.

### 🎨 Beautiful UI
Built with Snowflake's brand guidelines in mind, it adapts seamlessly to light and dark themes. It's the kind of dashboard you'll actually *want* to open.

### 🚀 Zero Maintenance
A scheduled Snowflake task refreshes your 30-day access snapshot every Sunday morning. Set it and forget it.

## The Magic Behind It: Why Snowflake Makes This Possible

Let me geek out for a moment about why Snowflake is uniquely positioned to enable this kind of visibility:

### 1. **Comprehensive Observability**
Snowflake's `account_usage` views (`sessions`, `query_history`, `access_history`) capture *everything*. Not just what happened, but the context around it. This isn't an afterthought—it's baked into the platform's architecture.

### 2. **Horizon Catalog**
Snowflake Horizon provides a unified view of your entire data landscape. It's not just access logs; it's lineage, governance, and observability in one place. Data Lake Explorer taps into this to give you the 30,000-foot view you need.

### 3. **Streamlit in Snowflake**
You can deploy this app *directly inside Snowflake*. No external hosting, no VPCs to configure, no security headaches. Your data never leaves your Snowflake environment. It's secure by default.

### 4. **Snowpark for Python**
The seamless integration between Streamlit and Snowflake through Snowpark means the app automatically gets a database session—no connection strings, no credential management. It just *works*.

### 5. **Tasks & Automation**
Snowflake's native task scheduling handles the data refresh pipeline. No Airflow, no cron jobs, no external orchestration needed. Pure simplicity.

## Real-World Use Cases

Since building this, I've found it invaluable for:

- **Audit & Compliance**: "Show me every application that accessed our PII database in the last 30 days"
- **Cost Optimization**: Identify unused databases consuming warehouse credits
- **Migration Planning**: Understand dependencies before deprecating old data sources  
- **Security Analysis**: Spot unusual access patterns or unexpected client types
- **Stakeholder Communication**: Show executives *exactly* how your data is being used (they love graphs)

## How to Get Started

Want to try it yourself? It takes about 5 minutes:

### Quick Install

```bash
# Clone the repository
git clone https://github.com/sfc-gh-mfulkerson/data-lake-explorer.git
cd data-lake-explorer

# Set up your Snowflake connection (one-time)
snow connection add

# Deploy everything
./deploy.sh <connection_name> <warehouse_name>
```

That's it. The deployment script will:
1. Create the database and schema
2. Set up the data refresh task  
3. Load your initial 30-day snapshot
4. Deploy the Streamlit app to Snowflake

### Try It Locally (No Snowflake Required)

Want to see it in action first? Run it locally with sample data:

```bash
# Install dependencies
pip install -r requirements.txt

# Run the app
streamlit run streamlit_app.py
```

The app will automatically use sample data for demonstration purposes.

### Check It Out on GitHub

🔗 **[github.com/sfc-gh-mfulkerson/data-lake-explorer](https://github.com/sfc-gh-mfulkerson/data-lake-explorer)**

## The Tech Stack

For those curious about the implementation:

- **[Streamlit](https://streamlit.io/)**: Turns Python scripts into interactive web apps
- **[PyVis](https://pyvis.readthedocs.io/)**: Powers the network graph visualization  
- **[Altair](https://altair-viz.github.io/)**: Creates the declarative bar charts
- **[Snowpark for Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)**: Handles Snowflake connectivity

## What's Next?

I'm actively developing new features, including:
- Time-based filtering (not just 30 days)
- User-level access patterns  
- Export capabilities for compliance reporting
- Custom alerting for unusual access patterns

Have ideas? Contributions are welcome! Open an issue or PR on GitHub.

## Final Thoughts: Data Observability as a First-Class Citizen

The best tools are the ones that make complex problems feel simple. Snowflake gives us the raw materials—comprehensive logging, powerful compute, and seamless application hosting. Data Lake Explorer is just one example of what's possible when you combine those ingredients.

Your data estate is a living, breathing ecosystem. You should be able to *see* it, understand it, and manage it without drowning in SQL queries.

That's what I built this for. I hope you find it as useful as I have.

---

*Have questions or want to share how you're using Data Lake Explorer? Drop a comment below or reach out on GitHub!*

---

**About the Project**

- 📦 **Project**: Data Lake Explorer  
- 🔗 **GitHub**: [sfc-gh-mfulkerson/data-lake-explorer](https://github.com/sfc-gh-mfulkerson/data-lake-explorer)
- 🏗️ **Built With**: Streamlit, Snowflake, PyVis, Altair
- 📄 **License**: See repository for details

---

*Tags: #Snowflake #DataEngineering #DataVisualization #DataObservability #Streamlit #Python #DataGovernance #Analytics*
