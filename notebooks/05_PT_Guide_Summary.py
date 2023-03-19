# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Spark_Price_Transparency Data Model Summary
# MAGIC 
# MAGIC If you've gone through the first five notebooks you saw: 
# MAGIC 
# MAGIC  * <a href="$./00_Guide_Data_Architecture" target="_blank">00_Guide_Data_Architecture</a> This provides the high level curation that we employ in Spark Price Transparency and show how to get started, `guide.initialize`.
# MAGIC  * <a href="$./01_Table-of-Contents" target="_blank">01_Table-of-Contents</a> Shows our first schema curations, **Table-of-Contents** which will provide urls for In-Network-Rates and Allowed-Amounts.
# MAGIC  * <a href="$./02_In-Network-Rates" target="_blank">02_In-Network-Rates</a> Shows the curation process for the most complicated schema, **In-Network-Rates**, where provider details can be applied in three different locations.
# MAGIC  * <a href="$./03_Provider-Reference" target="_blank">03_Guide_Provider-Reference</a> Shows how to manage **Provider-Reference** files if the reporting legal entity chooses to write them separate from the In-Network-Rates files.
# MAGIC  * <a href="$./04_Allowed-Amounts" target="_blank">04_Guide_Allowed-Amounts</a> Shows workflow for **Allowed-Amounts**.
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC While we know all the components that go into curating each of the schemas, its worthwhile to take a step back and look at how these schemas are dependent upon one another as we looks to build out jobs. Specifically, if you were to instead of looking at the workflow within each schema, you looked at the workflow across all schemas, you can see another level of complexity where we go from files to tables to file to tables to files to tables. This type of workflow is especially well suited for Databricks because this would be very diffifult to manage in a legacy Data Warehouse.

# COMMAND ----------

from spark_price_transparency.end2end_viz import get_end2end_html
displayHTML(get_end2end_html())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Databricks Unified Data Analytics Platform
# MAGIC 
# MAGIC Choosing Databricks for your Price Transparancy Applications goes beyond ETL data storage. By leveraging [DeltaLake](https://www.databricks.com/product/delta-lake-on-databricks) within Databricks your data is immediately available in Databricks analytic products; this Unified Data and Analytics Platform concept is called [Lakehouse](https://www.databricks.com/glossary/unified-data-analytics-platform). While evaluating Price Transparency Data, also check out tools for other personas, including:
# MAGIC 
# MAGIC  - [Dashboards](./sql/dashboards) for your business users
# MAGIC  - [SQL Editor](./sql/editor) for your buiness analyts
# MAGIC  - [Models](./#ml/dashboard) in MLFlow for your Data Scientists
# MAGIC  
# MAGIC  

# COMMAND ----------

"""
Unified Data and Analytics Views for price transparency
"""

def get_unified_html(cat_name='hive_metastore'):
    import re
    from graphviz import Digraph

    dot = Digraph('pt')
    dot.attr(compound='true')
    dot.graph_attr['rankdir'] = 'LR'
    dot.edge_attr.update(arrowhead='none', arrowsize='2')
    dot.attr('node', shape='rectangle')

    def tbl_link(wh_name, tbl_name, ttip=''):
        return {'tooltip': ttip, 'href': f'./explore/data/{cat_name}/{wh_name}/{tbl_name}', 'target': "_blank",
                'width': "1.5"}

    with dot.subgraph(name='cluster_workflow') as c:
        c.body.append('label="Price Transparency Workflows"')
        with c.subgraph(name='cluster_pt_raw') as r:
            r.body.append('label="Database: pt_raw"')
            r.body.append('style="filled"')
            r.body.append('color="#808080"')
            r.body.append('fillcolor="#F5F5F5"')
            with r.subgraph(name='cluster_pt_raw_toc') as rtoc:
                rtoc.body.append('label="Table-of-Contents Files"')
                rtoc.body.append('style="filled"')
                rtoc.body.append('color="#808080"')
                rtoc.body.append('fillcolor="#DCDCDC"')
                rtoc.node('toc_meta', 'guide.pt_raw.table-of-contents.meta', fillcolor='#F5F5F5', style='filled', shape='tab',**{'width': "4"})
                rtoc.node('table_of_contents_file', '.../schema=table-of-contents/*.json', fillcolor='#FFFACD',
                          style='filled', shape='folder', **{'width': "4"})
            with r.subgraph(name='cluster_pt_raw_inr') as rinr:
                rinr.body.append('label="In-Network-Rates Files"')
                rinr.body.append('style="filled"')
                rinr.body.append('color="#808080"')
                rinr.body.append('fillcolor="#DCDCDC"')
                rinr.node('inr_meta', 'guide.pt_raw.in-network-rates.meta', fillcolor='#F5F5F5', style='filled', shape='tab',**{'width': "4"})
                rinr.node('in_network_file', '.../schema=in-network-rates/*.json', fillcolor='#FFFACD', style='filled',
                          shape='folder', **{'width': "4"})
            with r.subgraph(name='cluster_pt_raw_pr') as rpr:
                rpr.body.append('label="Provider-Reference Files"')
                rpr.body.append('style="filled"')
                rpr.body.append('color="#808080"')
                rpr.body.append('fillcolor="#DCDCDC"')
                rpr.node('pr_meta', 'guide.provider-reference.meta', fillcolor='#F5F5F5', style='filled', shape='tab',**{'width': "4"})
                rpr.node('provider_reference_file', '.../schema=provider-reference/*.json', fillcolor='#FFFACD',
                         style='filled', shape='folder', **{'width': "4"})
            with r.subgraph(name='cluster_pt_raw_aa') as raa:
                raa.body.append('label="Allowed-Amount Files"')
                raa.body.append('style="filled"')
                raa.body.append('color="#808080"')
                raa.body.append('fillcolor="#DCDCDC"')
                raa.node('aa_meta', 'guide.pt_raw.allowed-amounts.meta', fillcolor='#F5F5F5', style='filled', shape='tab', **{'width': "4"})
                raa.node('allowed_amount_file', '.../schema=allowed-amount/*.json', fillcolor='#FFFACD', style='filled',
                         shape='folder', **{'width': "4"})
        with c.subgraph(name='cluster_pt_stage') as s:
            s.body.append('label="Database: pt_stage"')
            s.body.append('style="filled"')
            s.body.append('color="#808080"')
            s.body.append('fillcolor="#F5F5F5"')

            s.node('toc', '', fillcolor='#CAD9EF', style='filled', shape='point')
            s.node('toc_inr', '', fillcolor='red', style='invis', shape='point')
            s.node('inr', '', fillcolor='#CAD9EF', style='filled', shape='point')
            s.node('inr', '', fillcolor='#CAD9EF', style='filled', shape='point')
            s.node('pr', '', fillcolor='#CAD9EF', style='filled', shape='point')
            s.node('pr_aa', '', fillcolor='red', style='invis', shape='point')
            s.node('aa', '', fillcolor='#CAD9EF', style='filled', shape='point')

            s.node('toc_header', 'toc_header', fillcolor='#CAD9EF', style='filled',
                   **tbl_link('pt_stage', 'toc_header'))
            s.node('toc_reporting', 'toc_reporting', fillcolor='#CAD9EF', style='filled',
                   **tbl_link('pt_stage', 'toc_reporting'))
            s.node('toc_inr_ingest', 'toc_inr_ingest', fillcolor='red', style='invis', **{'width': "2"})
            s.node('inr_header', 'inr_header', fillcolor='#CAD9EF', style='filled',
                   **tbl_link('pt_stage', 'inr_header'))
            s.node('inr_network', 'inr_network', fillcolor='#CAD9EF', style='filled',
                   **tbl_link('pt_stage', 'inr_network'))
            s.node('inr_provider', 'inr_provider', fillcolor='#CAD9EF', style='filled',
                   **tbl_link('pt_stage', 'inr_provider'))
            s.node('pr_provider', 'pr_provider', fillcolor='#CAD9EF', style='filled',
                   **tbl_link('pt_stage', 'pr_provider'))
            s.node('pr_aa_ingest', 'pr_aa_ingest', fillcolor='red', style='invis', **{'width': "2"})
            s.node('aa_header', 'aa_header', fillcolor='#CAD9EF', style='filled', **tbl_link('pt_stage', 'aa_header'))
            s.node('aa_network', 'aa_network', fillcolor='#CAD9EF', style='filled',
                   **tbl_link('pt_stage', 'aa_network'))

            s.node('top_analytic', 'top_analytic', fillcolor='red', style='invis', **{'width': "2"})
            s.node('index_reports', 'index_reports', fillcolor='#CAD9EF', style='filled',
                   **tbl_link('pt_stage', 'index_reports'))
            s.node('toc_inr_analytic', 'toc_inr_analytic', fillcolor='red', style='invis', **{'width': "2"})
            s.node('in_coverage', 'in_coverage', fillcolor='#CAD9EF', style='filled',
                   **tbl_link('pt_stage', 'in_coverage'))
            s.node('in_rate', 'in_rate', fillcolor='#CAD9EF', style='filled', **tbl_link('pt_stage', 'in_rate'))
            s.node('in_provider', 'in_provider', fillcolor='#CAD9EF', style='filled',
                   **tbl_link('pt_stage', 'in_provider'))
            s.node('in_pr_loc', 'in_pr_loc', fillcolor='#CAD9EF', style='filled',
                   **tbl_link('pt_stage', 'in_pr_loc'))
            s.node('pr_aa_analytic', 'pr_aa_analytic', fillcolor='red', style='invis', **{'width': "2"})
            s.node('out_code', 'out_code', fillcolor='#CAD9EF', style='filled', **tbl_link('pt_stage', 'out_code'))
            s.node('out_amount', 'out_amount', fillcolor='#CAD9EF', style='filled',
                   **tbl_link('pt_stage', 'out_amount'))

        with c.subgraph(name='cluster_pt') as p:
            p.body.append('label="Database: pt"')
            p.body.append('style="filled"')
            p.body.append('color="#808080"')
            p.body.append('fillcolor="#F5F5F5"')

            p.node('v_top_analytic', 'v_top_analytic', fillcolor='red', style='invis', **{'width': "2"})
            p.node('v_index_reports', 'index_reports', fillcolor='#CAD9EF', style='filled,dashed',
                   **tbl_link('pt', 'index_reports'))
            p.node('v_toc_inr_analytic', 'toc_inr_analytic', fillcolor='red', style='invis', **{'width': "2"})
            p.node('v_in_coverage', 'in_coverage', fillcolor='#CAD9EF', style='filled,dashed',
                   **tbl_link('pt', 'in_coverage'))
            p.node('v_in_rate', 'in_rate', fillcolor='#CAD9EF', style='filled,dashed', **tbl_link('pt', 'in_rate'))
            p.node('v_in_provider', 'in_provider', fillcolor='#CAD9EF', style='filled,dashed',
                   **tbl_link('pt', 'in_provider'))
            p.node('v_pr_aa_analytic1', 'pr_aa_analytic1', fillcolor='red', style='invis', **{'width': "2"})
            p.node('v_pr_aa_analytic2', 'pr_aa_analytic2', fillcolor='red', style='invis', **{'width': "2"})
            p.node('v_out_code', 'out_code', fillcolor='#CAD9EF', style='filled,dashed', **tbl_link('pt', 'out_code'))
            p.node('v_out_amount', 'out_amount', fillcolor='#CAD9EF', style='filled,dashed',
                   **tbl_link('pt', 'out_amount'))
            p.node('operation', 'Operation\nTables', shape='square', fillcolor='#CAD9EF', style='filled,dashed', **{'width': "1.5"})
            p.node('insight', 'Insight\nTables', shape='square', fillcolor='#CAD9EF', style='filled,dashed', **{'width': "1.5"})
            p.node('model', 'Model\nTables', shape='square', fillcolor='#CAD9EF', style='filled,dashed', **{'width': "1.5"})

        dot.node('dashboards', label='Dashboards', **{'fontsize': "48", 'width': "4.2", 'height': "1.5"})
        dot.node('sql', label='SQL Editor', **{'fontsize': "48", 'width': "4.2", 'height': "1.5"})        
        dot.node('models', label='Models', **{'fontsize': "48", 'width': "4.2", 'height': "1.5"})        
        
            
        dot.edge('table_of_contents_file', 'toc', ltail='cluster_pt_raw_toc')
        dot.edge('table_of_contents_file', 'toc_inr', ltail='cluster_pt_raw_toc', style="invis")
        dot.edge('in_network_file', 'toc_inr', ltail='cluster_pt_raw_inr', style="invis")
        dot.edge('in_network_file', 'inr', ltail='cluster_pt_raw_inr')
        dot.edge('provider_reference_file', 'pr', ltail='cluster_pt_raw_pr')
        dot.edge('provider_reference_file', 'pr_aa', ltail='cluster_pt_raw_pr', style="invis")
        dot.edge('allowed_amount_file', 'pr_aa', ltail='cluster_pt_raw_aa', style="invis")
        dot.edge('allowed_amount_file', 'aa', ltail='cluster_pt_raw_aa')

        dot.edge('toc', 'toc_header')
        dot.edge('toc', 'toc_reporting')
        dot.edge('toc_inr', 'toc_reporting', style="invis")
        dot.edge('toc_inr', 'toc_inr_ingest', style="invis")
        dot.edge('toc_inr', 'inr_header', style="invis")
        dot.edge('toc_inr', 'inr_header', style="invis")
        dot.edge('toc_inr', 'inr_header', style="invis")
        dot.edge('inr', 'inr_header')
        dot.edge('inr', 'inr_network')
        dot.edge('inr', 'inr_provider')
        dot.edge('pr', 'pr_provider', style="invis")
        dot.edge('pr', 'pr_provider')
        dot.edge('pr', 'pr_provider', style="invis")
        dot.edge('pr_aa', 'pr_aa_ingest', style="invis")
        dot.edge('pr_aa', 'aa_header', style="invis")
        dot.edge('aa', 'aa_header')
        dot.edge('aa', 'aa_network')

        dot.edge('toc_header', 'top_analytic', style="invis")
        dot.edge('toc_header', 'index_reports')
        dot.edge('toc_reporting', 'index_reports')
        dot.edge('toc_inr_ingest', 'top_analytic', style="invis")
        dot.edge('toc_inr_ingest', 'toc_inr_analytic', style="invis")
        dot.edge('inr_header', 'in_coverage')
        dot.edge('inr_network', 'in_coverage')
        dot.edge('inr_header', 'in_rate')
        dot.edge('inr_network', 'in_rate')
        dot.edge('inr_provider', 'in_rate')
        dot.edge('inr_provider', 'in_pr_loc')
        dot.edge('inr_network', 'in_provider')
        dot.edge('inr_header', 'in_provider')
        dot.edge('inr_provider', 'in_provider')
        dot.edge('pr_provider', 'in_rate', style='dashed', color='blue')
        dot.edge('pr_provider', 'in_provider', style='dashed', color='blue')
        dot.edge('pr_provider', 'in_pr_loc', style="invis")
        dot.edge('inr_header', 'in_pr_loc')
        dot.edge('pr_aa_ingest', 'pr_aa_analytic', style="invis")
        dot.edge('aa_header', 'out_code')
        dot.edge('aa_network', 'out_amount')
        dot.edge('aa_network', 'out_code')
        dot.edge('aa_header', 'out_amount')

        dot.edge('top_analytic', 'v_top_analytic', style="invis")
        dot.edge('index_reports', 'v_index_reports')
        dot.edge('toc_inr_analytic', 'v_toc_inr_analytic', style="invis")
        dot.edge('in_coverage', 'v_in_coverage')
        dot.edge('in_coverage', 'v_in_rate', style="invis")
        dot.edge('in_rate', 'v_in_rate')
        dot.edge('in_provider', 'v_in_provider')
        dot.edge('pr_aa_analytic', 'v_pr_aa_analytic1', style="invis")
        dot.edge('pr_aa_analytic', 'v_pr_aa_analytic2', style="invis")
        dot.edge('pr_aa_analytic', 'v_out_code', style="invis")
        dot.edge('out_code', 'v_out_code')
        dot.edge('out_amount', 'v_out_amount')
        
        dot.edge('v_top_analytic', 'operation', style="invis")
        dot.edge('v_index_reports', 'operation', style="invis")
        dot.edge('v_toc_inr_analytic', 'operation', style="invis")
        dot.edge('v_in_coverage', 'operation', style="invis")
        dot.edge('v_in_rate', 'operation', style="invis")        
        
        dot.edge('v_in_coverage', 'insight', style="invis")
        dot.edge('v_in_rate', 'insight', style="invis")
        dot.edge('v_in_provider', 'insight', style="invis")
        dot.edge('v_pr_aa_analytic2', 'insight', style="invis")
        
        dot.edge('v_pr_aa_analytic1', 'model', style="invis")
        dot.edge('v_pr_aa_analytic2', 'model', style="invis")
        dot.edge('v_out_code', 'model', style="invis")
        dot.edge('v_out_amount', 'model', style="invis")
        
        dot.edge('operation', 'dashboards', arrowhead='normal')
        dot.edge('insight', 'sql', arrowhead='normal')
        dot.edge('model', 'models', arrowhead='normal')
        

    html = dot._repr_image_svg_xml()

    html = re.sub(r'<svg width=\"\d*pt\" height=\"\d*pt\"',
                  '<div style="text-align:center;"><svg width="700pt" aligned=center', html)
    html = re.sub(r'Price Transparency Workflow',
                  '<a href="https://pypi.org/project/spark-price-transparency/" target="_blank">Price Transparency Workflow</a>',
                  html)
    html = re.sub(r'Database: pt_raw',
                  f'<a href="./explore/data/{cat_name}/pt_raw" target="_blank">Database&#58; pt&#95;raw</a>',
                  html)
    html = re.sub(r'Database: pt_stage',
                  f'<a href="./explore/data/{cat_name}/pt_stage" target="_blank">Database&#58; pt&#95;stage</a>',
                  html)
    html = re.sub(r'Database: pt',
                  f'<a href="./explore/data/{cat_name}/pt" target="_blank">Database&#58; pt</a>',
                  html)
    html = re.sub(r'Table&#45;of&#45;Contents Files',
                  '<a href="https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/table-of-contents" target="_blank">Table&#45;of&#45;Contents Files</a>',
                  html)
    html = re.sub(r'In&#45;Network&#45;Rates Files',
                  '<a href="https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/in-network-rates" target="_blank">In&#45;Network&#45;Rates Files</a>',
                  html)
    html = re.sub(r'Provider&#45;Reference Files',
                  '<a href="https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/provider-reference" target="_blank">Provider&#45;Reference Files</a>',
                  html)
    html = re.sub(r'Allowed&#45;Amount Files',
                  '<a href="https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/allowed-amounts" target="_blank">Allowed&#45;Amount Files</a>',
                  html)
    
    html = re.sub(r'stroke-width=\"2\"', 'stroke-width=\"4\"', html)

    return html

displayHTML(get_unified_html())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Spark_Price_Transparency Appendices
# MAGIC 
# MAGIC The following are provided as appendices since these will be of interest for Price Transparency application developers, but are not critical to understanding the spark_price_transparency data and curation concepts. The first is a delta share of some sample data if you want to first do discovery by looking at some data:
# MAGIC 
# MAGIC  * <a href="$./A1_Delta_Share" target="_blank">A1_Delta_Share</a> We provide a notebook to explore sample data created using spark_price_transparency. This provides a brief explaination on how to access the data and allows the developers to explore the spark_price_transparency notebooks through temporary table views. Included is a token that is good through **31 MAR 2023**.
# MAGIC  * <a href="$./A2_Dashboard" target="_blank">A2_Dashboard</a> Building on the Unified Data Analytic Platform concept above, this is a walkthrough of how to build a Dashboard from a Price Transparency Data source. You can even evaluate this using the Delta Share connection.
# MAGIC  * <a href="$./A3_Machine_Learning" target="_blank">A3_Machine_Learning</a> Building on the Unified Data Analytics Platform concept above, this is a walkthrough on how to build an unsupervised learning model based upon price transparency data.
# MAGIC  * <a href="$./A4_Anthem_Workflow" target="_blank">A4_Anthem_Workflow</a> Not all price transparency source data will be the same. They can differ based upon decisions of where to provide provider details and how to organize table-of-contents files. The Anthem Workflow is provided to show that the spark_price_transparency solution works across the approached used by source file producers.
# MAGIC  * <a href="$./A5_UHG_Workflow" target="_blank">A5_UHG_Workflow</a> Not all price transparency source data will be the same. They can differ based upon decisions of where to provide provider details and how to organize table-of-contents files. The UHG Workflow is provided to show that the spark_price_transparency solution works across the approached used by source file producers.
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Feature Roadmap:
# MAGIC 
# MAGIC In development as of 21 MAR 2023:
# MAGIC 
# MAGIC  * [Concurrent streaming ingests for all run_ingest variant methods and all schema types](https://github.com/databricks/hls-price-transparency/issues/7)
# MAGIC  * [Concurrent import across workers for all run_import variant methods and all schema types](https://github.com/databricks/hls-price-transparency/issues/4)
# MAGIC  * [Run import methods accept either file_name or file_path as an argument](https://github.com/databricks/hls-price-transparency/issues/5)
# MAGIC  * [Add UC catalog and external location support](https://github.com/databricks/hls-price-transparency/issues/2)
# MAGIC  * [Remove graphviz dependency and render all graphics from html template](https://github.com/databricks/hls-price-transparency/issues/3)
# MAGIC 
# MAGIC Contact your account sales team if interested in being one of the first to evaluate the above features.
# MAGIC  
# MAGIC  **NOTE**: Links will not be publicly accessible until the spark_price_transparency repo is made public. Spark_Price_Transparency uses a Databricks Labs License.
