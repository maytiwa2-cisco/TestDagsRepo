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
        """Return list of targets to scan"""
        return ["scanme.nmap.org", "example.com", "testsite.com"]
    
    @task.kubernetes(
        image="instrumentisto/nmap:latest",
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
    def aggregate_results(scan_results: list):
        """Aggregate all scan results"""
        print(f"Processed {len(scan_results)} scans")
        logger.info(f"Scan Results: {scan_results}")
        return {
            "total_scans": len(scan_results),
            "results": scan_results
        }
    
    # Dynamic task mapping - creates one pod per target
    targets = get_scan_targets()
    scans = run_nmap_scan.expand(target=targets)
    final = aggregate_results(scans)

nmap_kubernetes_multi_scan()