
<table>
    <tr>
        <td><img src="./img/icon-orange-healthcare_0.svg" alt="hc_icon"></td>
        <td align="center">Building a Lakehouse for Healthcare: <br/>Unlocking Price Transparency</td>
        <td><img src="./img/health_care_accelerator.jpeg" alt="hls_acc_icon"></td>
    </tr>
</table>

---

# Spark Price Transparency Workshop

<img src="./img/icon-orange-healthcare_0.svg" alt="hc_icon">

This even repo contains material shared during the 21 MAR 2023. 



https://www.databricks.com/solutions/accelerators/price-transparency-data

[![Spark](https://img.shields.io/badge/Spark-3.2.1-orange)](https://docs.databricks.com/release-notes/runtime/releases.html)
[![DatabricksRuntime](https://img.shields.io/badge/Databricks%20Runtime-10.4%20LTS-orange)](https://docs.databricks.com/release-notes/runtime/releases.html)
[![PyPi](https://img.shields.io/pypi/v/spark-price-transparency)](https://pypi.org/project/spark-price-transparency)
[![PyPi](https://img.shields.io/pypi/wheel/spark-price-transparency)](https://pypi.org/project/spark-price-transparency)


---

## Cluster Dependencies:
   TODO: Test and update readme after public, remove alternates if workshop releases work
   For the notebooks to run as intended, you will need to configure three dependencies on your cluster:
 * [payer-mrf-streamsource-0.3.5.jar](https://github.com/databricks-industry-solutions/hls-price-transparency-workshop/releases/tag/v0.3.5-workshop) - Go to your cluster -> Libraries -> Install New -> Maven and set:
    * **Coordinates**: `databricks-industry-solutions:hls-price-transparency-workshop:0.3.5`
    * **Repository**:  `https://github.com/databricks-industry-solutions/hls-price-transparency-workshop/raw/maven`
    Alternate:
    * **Coordinates**: `databricks-industry-solutions:hls-payer-mrf-sparkstreaming:0.3.5`
    * **Repository**:  `https://github.com/databricks-industry-solutions/hls-payer-mrf-sparkstreaming/raw/maven`
 * [spark-price-transparency-0.1.85-py3-none-any.whl]() - Go to your cluster -> Libraries -> Install New -> PyPI and set:
    * **Package**: `spark-price-transparency==0.1.85`
    * **Repository**: `https://github.com/databricks-industry-solutions/hls-price-transparency-workshop/v0.1.85/spark-price-transparency-0.1.85-py3-none-any.whl`
 * [install_graphviz.sh])(https://github.com/databricks-industry-solutions/hls-price-transparency-workshop/tree/main/init_scripts) - Download and install. Go to your cluster -> Cluster -> Configuration -> Advanced Options -> Init Scripts.  
   

