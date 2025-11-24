from airflow.decorators import dag, task
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

@dag(
    dag_id="nmap_kubernetes_multi_scan",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nmap", "kubernetes"],
)
def nmap_kubernetes_multi_scan():
    
    @task
    def get_scan_targets():
        """Return target to scan"""
        return "scanme.nmap.org"
    
    @task.kubernetes(
        image="mayankt23/airflow-nmap:latest",
        name="nmap-scan",
        namespace="airflow",
    )
    def run_nmap_scan(target: str):
        import subprocess
        import json
        
        result = subprocess.run(
            ["nmap", "-sV", "-oX", "-", target],
            capture_output=True,
            text=True,
            check=True
        )
        
        return {
            "status": "completed",
            "target": target,
            "scan_results": result.stdout,
            "format": "xml"
        }
    
    @task
    def aggregate_results(scan_results: dict):
        logger.info(f"Scan Results: {scan_results["scan_results"]} for target {scan_results["target"]}")
        return scan_results["scan_results"]
    
    # Dynamic task mapping - creates one pod per target
    target = get_scan_targets()
    scans = run_nmap_scan(target)
    final = aggregate_results(scans)

nmap_kubernetes_multi_scan()