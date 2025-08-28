"""
Documentation Generator
Handles generation and formatting of validation results documentation
"""

import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from io import StringIO

from ..utils.logger import setup_logger
from ..exceptions.custom_exceptions import StorageError
from ..storage.adls_client import ADLSClient

logger = setup_logger(__name__)


class DocumentationGenerator:
    """Generates formatted documentation for validation results"""
    
    def __init__(self, storage_config: Dict[str, Any], adls_client: Optional[ADLSClient] = None):
        """
        Initialize documentation generator
        
        Args:
            storage_config (Dict[str, Any]): Storage configuration
            adls_client (Optional[ADLSClient]): ADLS client for uploading docs
        """
        self.storage_config = storage_config
        self.adls_client = adls_client
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
    def generate_html_report(self, validation_results: List[Dict[str, Any]]) -> str:
        """
        Generate comprehensive HTML report for validation results
        
        Args:
            validation_results (List[Dict[str, Any]]): Validation results to document
            
        Returns:
            str: Generated HTML content
            
        Raises:
            StorageError: If report generation or upload fails
        """
        try:
            logger.info("Generating HTML validation report...")
            
            html_content = self._create_html_report(validation_results)
            
            # Upload to ADLS if client is available
            if self.adls_client:
                doc_path = self.adls_client.upload_documentation(
                    html_content, 
                    "validation_report", 
                    self.timestamp
                )
                logger.info(f"HTML report uploaded to: {doc_path}")
            else:
                # Save locally if no ADLS client
                local_path = f"/tmp/validation_report_{self.timestamp}.html"
                with open(local_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                logger.info(f"HTML report saved locally: {local_path}")
            
            return html_content
            
        except Exception as e:
            error_msg = f"Error generating HTML report: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def _create_html_report(self, validation_results: List[Dict[str, Any]]) -> str:
        """
        Create HTML report content
        
        Args:
            validation_results (List[Dict[str, Any]]): Validation results
            
        Returns:
            str: HTML content
        """
        html = StringIO()
        
        # HTML header
        html.write("""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Quality Validation Report</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .header {
            text-align: center;
            margin-bottom: 30px;
            border-bottom: 2px solid #e9ecef;
            padding-bottom: 20px;
        }
        .header h1 {
            color: #343a40;
            margin: 0;
        }
        .header p {
            color: #6c757d;
            margin: 10px 0 0 0;
        }
        .summary {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        .summary-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 6px;
            text-align: center;
        }
        .summary-card.success {
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        }
        .summary-card.error {
            background: linear-gradient(135deg, #ff9a9e 0%, #fecfef 100%);
        }
        .summary-card h3 {
            margin: 0 0 10px 0;
            font-size: 2em;
        }
        .summary-card p {
            margin: 0;
            opacity: 0.9;
        }
        .table-section {
            margin-bottom: 40px;
        }
        .table-header {
            background-color: #f8f9fa;
            padding: 15px;
            border-left: 4px solid #007bff;
            margin-bottom: 20px;
        }
        .table-header h2 {
            margin: 0;
            color: #495057;
        }
        .table-header .status {
            display: inline-block;
            padding: 4px 12px;
            border-radius: 4px;
            font-size: 0.9em;
            font-weight: bold;
            margin-left: 10px;
        }
        .status.success {
            background-color: #d4edda;
            color: #155724;
        }
        .status.error {
            background-color: #f8d7da;
            color: #721c24;
        }
        .expectations-table {
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }
        .expectations-table th,
        .expectations-table td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #dee2e6;
        }
        .expectations-table th {
            background-color: #495057;
            color: white;
            font-weight: 600;
        }
        .expectations-table tr:hover {
            background-color: #f8f9fa;
        }
        .expectation-success {
            color: #28a745;
            font-weight: bold;
        }
        .expectation-failed {
            color: #dc3545;
            font-weight: bold;
        }
        .details {
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 4px;
            margin-top: 10px;
        }
        .details pre {
            background-color: white;
            padding: 10px;
            border-radius: 4px;
            overflow-x: auto;
            margin: 0;
        }
        .footer {
            text-align: center;
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #dee2e6;
            color: #6c757d;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Data Quality Validation Report</h1>
            <p>Generated on """ + datetime.now().strftime('%B %d, %Y at %I:%M %p') + """</p>
        </div>
""")
        
        # Summary section
        total_tables = len(validation_results)
        successful_tables = sum(1 for r in validation_results if r.get('success', False))
        failed_tables = total_tables - successful_tables
        
        html.write(f"""
        <div class="summary">
            <div class="summary-card">
                <h3>{total_tables}</h3>
                <p>Total Tables</p>
            </div>
            <div class="summary-card success">
                <h3>{successful_tables}</h3>
                <p>Successful Validations</p>
            </div>
            <div class="summary-card error">
                <h3>{failed_tables}</h3>
                <p>Failed Validations</p>
            </div>
            <div class="summary-card">
                <h3>{(successful_tables/total_tables*100):.1f}%</h3>
                <p>Success Rate</p>
            </div>
        </div>
""")
        
        # Individual table results
        for result in validation_results:
            table_name = result['table_name']
            success = result.get('success', False)
            status_class = 'success' if success else 'error'
            status_text = 'PASSED' if success else 'FAILED'
            
            html.write(f"""
        <div class="table-section">
            <div class="table-header">
                <h2>{table_name}</h2>
                <span class="status {status_class}">{status_text}</span>
            </div>
""")
            
            # Table details
            if 'details' in result and isinstance(result['details'], dict):
                details = result['details']
                
                if 'expectation_results' in details:
                    html.write("""
            <table class="expectations-table">
                <thead>
                    <tr>
                        <th>Expectation Type</th>
                        <th>Column</th>
                        <th>Status</th>
                        <th>Parameters</th>
                    </tr>
                </thead>
                <tbody>
""")
                    
                    for exp_result in details['expectation_results']:
                        exp_type = exp_result.get('expectation_type', 'Unknown')
                        column = exp_result.get('kwargs', {}).get('column', 'N/A')
                        exp_success = exp_result.get('success', False)
                        exp_status_class = 'expectation-success' if exp_success else 'expectation-failed'
                        exp_status_text = 'PASS' if exp_success else 'FAIL'
                        
                        # Format parameters
                        params = exp_result.get('kwargs', {})
                        param_text = ', '.join([f"{k}={v}" for k, v in params.items() if k != 'column'])
                        if not param_text:
                            param_text = 'None'
                        
                        html.write(f"""
                    <tr>
                        <td>{exp_type}</td>
                        <td>{column}</td>
                        <td class="{exp_status_class}">{exp_status_text}</td>
                        <td>{param_text}</td>
                    </tr>
""")
                    
                    html.write("""
                </tbody>
            </table>
""")
                
                # Summary statistics
                html.write(f"""
            <div class="details">
                <h4>Summary Statistics</h4>
                <p><strong>Total Expectations:</strong> {details.get('total_expectations', 0)}</p>
                <p><strong>Successful Expectations:</strong> {details.get('successful_expectations', 0)}</p>
                <p><strong>Failed Expectations:</strong> {details.get('failed_expectations', 0)}</p>
            </div>
""")
            
            # Error details for failed validations
            if 'error' in result:
                html.write(f"""
            <div class="details">
                <h4>Error Details</h4>
                <pre>{result['error']}</pre>
            </div>
""")
            
            html.write("        </div>")
        
        # HTML footer
        html.write("""
        <div class="footer">
            <p>This report was generated by the Great Expectations Data Quality Application</p>
            <p>For more information, contact your data engineering team</p>
        </div>
    </div>
</body>
</html>
""")
        
        return html.getvalue()
    
    def generate_summary_report(self, validation_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate summary report in JSON format
        
        Args:
            validation_results (List[Dict[str, Any]]): Validation results
            
        Returns:
            Dict[str, Any]: Summary report data
            
        Raises:
            StorageError: If summary generation or upload fails
        """
        try:
            logger.info("Generating summary validation report...")
            
            summary_data = self._create_summary_data(validation_results)
            
            # Upload to ADLS if client is available
            if self.adls_client:
                summary_path = f"validation_results/summary_{self.timestamp}.json"
                self.adls_client.upload_json(summary_data, summary_path)
                logger.info(f"Summary report uploaded to: {summary_path}")
            else:
                # Save locally if no ADLS client
                local_path = f"/tmp/validation_summary_{self.timestamp}.json"
                with open(local_path, 'w', encoding='utf-8') as f:
                    json.dump(summary_data, f, indent=2, default=str)
                logger.info(f"Summary report saved locally: {local_path}")
            
            return summary_data
            
        except Exception as e:
            error_msg = f"Error generating summary report: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
    
    def _create_summary_data(self, validation_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create summary data structure
        
        Args:
            validation_results (List[Dict[str, Any]]): Validation results
            
        Returns:
            Dict[str, Any]: Summary data
        """
        total_tables = len(validation_results)
        successful_tables = sum(1 for r in validation_results if r.get('success', False))
        failed_tables = total_tables - successful_tables
        
        # Calculate expectation statistics
        total_expectations = 0
        successful_expectations = 0
        failed_expectations = 0
        
        table_summaries = []
        
        for result in validation_results:
            table_summary = {
                'table_name': result['table_name'],
                'success': result.get('success', False),
                'timestamp': result.get('timestamp', ''),
                'run_id': result.get('run_id', '')
            }
            
            if 'details' in result and isinstance(result['details'], dict):
                details = result['details']
                table_total = details.get('total_expectations', 0)
                table_success = details.get('successful_expectations', 0)
                table_failed = details.get('failed_expectations', 0)
                
                table_summary.update({
                    'total_expectations': table_total,
                    'successful_expectations': table_success,
                    'failed_expectations': table_failed,
                    'success_rate': (table_success / table_total * 100) if table_total > 0 else 0
                })
                
                total_expectations += table_total
                successful_expectations += table_success
                failed_expectations += table_failed
            
            if 'error' in result:
                table_summary['error'] = result['error']
            
            table_summaries.append(table_summary)
        
        summary_data = {
            'report_metadata': {
                'generated_at': datetime.now().isoformat(),
                'generator': 'Great Expectations Data Quality Application',
                'report_type': 'validation_summary'
            },
            'overall_summary': {
                'total_tables': total_tables,
                'successful_tables': successful_tables,
                'failed_tables': failed_tables,
                'table_success_rate': (successful_tables / total_tables * 100) if total_tables > 0 else 0,
                'total_expectations': total_expectations,
                'successful_expectations': successful_expectations,
                'failed_expectations': failed_expectations,
                'expectation_success_rate': (successful_expectations / total_expectations * 100) if total_expectations > 0 else 0
            },
            'table_results': table_summaries,
            'recommendations': self._generate_recommendations(table_summaries)
        }
        
        return summary_data
    
    def _generate_recommendations(self, table_summaries: List[Dict[str, Any]]) -> List[str]:
        """
        Generate recommendations based on validation results
        
        Args:
            table_summaries (List[Dict[str, Any]]): Table summary data
            
        Returns:
            List[str]: List of recommendations
        """
        recommendations = []
        
        failed_tables = [t for t in table_summaries if not t['success']]
        low_success_tables = [t for t in table_summaries 
                             if t.get('success_rate', 100) < 80 and t['success']]
        
        if failed_tables:
            recommendations.append(
                f"Immediate attention required for {len(failed_tables)} tables with validation failures: "
                f"{', '.join([t['table_name'] for t in failed_tables[:3]])}"
                + ("..." if len(failed_tables) > 3 else "")
            )
        
        if low_success_tables:
            recommendations.append(
                f"Review data quality rules for {len(low_success_tables)} tables with low success rates: "
                f"{', '.join([t['table_name'] for t in low_success_tables[:3]])}"
                + ("..." if len(low_success_tables) > 3 else "")
            )
        
        if not failed_tables and not low_success_tables:
            recommendations.append("All tables passed validation successfully. Continue monitoring data quality trends.")
        
        recommendations.extend([
            "Implement automated alerting for critical validation failures",
            "Schedule regular review of expectation definitions",
            "Monitor data quality trends over time",
            "Document any expected data quality issues and their business impact"
        ])
        
        return recommendations
    
    def generate_csv_export(self, validation_results: List[Dict[str, Any]]) -> str:
        """
        Generate CSV export of validation results
        
        Args:
            validation_results (List[Dict[str, Any]]): Validation results
            
        Returns:
            str: CSV content
        """
        try:
            logger.info("Generating CSV export...")
            
            csv_content = StringIO()
            
            # Write header
            csv_content.write("table_name,validation_timestamp,overall_success,total_expectations,successful_expectations,failed_expectations,success_rate\n")
            
            # Write data rows
            for result in validation_results:
                table_name = result['table_name']
                timestamp = result.get('timestamp', '')
                success = result.get('success', False)
                
                if 'details' in result and isinstance(result['details'], dict):
                    details = result['details']
                    total_exp = details.get('total_expectations', 0)
                    success_exp = details.get('successful_expectations', 0)
                    failed_exp = details.get('failed_expectations', 0)
                    success_rate = (success_exp / total_exp * 100) if total_exp > 0 else 0
                else:
                    total_exp = success_exp = failed_exp = success_rate = 0
                
                csv_content.write(f"{table_name},{timestamp},{success},{total_exp},{success_exp},{failed_exp},{success_rate:.1f}\n")
            
            csv_data = csv_content.getvalue()
            
            # Upload to ADLS if client is available
            if self.adls_client:
                csv_path = f"validation_results/export_{self.timestamp}.csv"
                self.adls_client.upload_text(csv_data, csv_path, 'text/csv')
                logger.info(f"CSV export uploaded to: {csv_path}")
            
            return csv_data
            
        except Exception as e:
            error_msg = f"Error generating CSV export: {e}"
            logger.error(error_msg)
            raise StorageError(error_msg)
