# hls-price-transparency-workshop
Public workshop material for 2023 Q2

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
    
   

