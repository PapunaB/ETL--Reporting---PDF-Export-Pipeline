"""
Report Generator Module

This module provides functionality to generate reports from the database.
"""

import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
from datetime import datetime
from pathlib import Path
from etl_pipeline.utils.database import DatabaseTransaction

logger = logging.getLogger('etl_pipeline.reports.generator')


class ReportGenerator:
    """Class for generating reports from the database."""
    
    def __init__(self, connection, db_type='sqlite', reports_dir='reports'):
        """
        Initialize with a database connection and reports directory.
        
        Args:
            connection: A database connection object
            db_type (str): Type of database ('sqlite' or 'postgresql')
            reports_dir (str): Directory to save reports
        """
        self.conn = connection
        self.db_type = db_type.lower()
        self.reports_dir = reports_dir
        
        # Create reports directory if it doesn't exist
        Path(reports_dir).mkdir(exist_ok=True)
    
    def generate_reports(self):
        """
        Generate reports from the database.
        
        Returns:
            bool: True if reports were generated successfully, False otherwise
        
        Raises:
            Exception: If an error occurs during report generation
        """
        try:
            logger.info("Starting report generation")
            
            # Run queries
            # 1. Total sales by affiliate
            affiliate_sales = pd.read_sql_query(
                "SELECT affiliate_name, SUM(sales_amount_usd) as total_sales_usd FROM sales GROUP BY affiliate_name ORDER BY total_sales_usd DESC",
                self.conn
            )
            
            # 2. Total sales by category
            category_sales = pd.read_sql_query(
                "SELECT category, SUM(sales_amount_usd) as total_sales_usd FROM sales GROUP BY category ORDER BY total_sales_usd DESC",
                self.conn
            )
            
            # 3. Monthly sales summary
            monthly_sales = pd.read_sql_query(
                "SELECT month, SUM(sales_amount_usd) as total_sales_usd FROM sales GROUP BY month ORDER BY month",
                self.conn
            )
            
            # 4. Overall summary
            summary = pd.read_sql_query(
                """
                SELECT 
                    COUNT(order_id) as total_orders,
                    SUM(sales_amount_usd) as total_sales_usd,
                    AVG(sales_amount_usd) as avg_order_value_usd,
                    MIN(sales_amount_usd) as min_order_value_usd,
                    MAX(sales_amount_usd) as max_order_value_usd
                FROM sales
                """,
                self.conn
            )
            
            # Generate CSV reports
            logger.info("Generating CSV reports")
            affiliate_sales.to_csv(f"{self.reports_dir}/affiliate_sales.csv", index=False)
            category_sales.to_csv(f"{self.reports_dir}/category_sales.csv", index=False)
            monthly_sales.to_csv(f"{self.reports_dir}/monthly_sales.csv", index=False)
            
            # Store aggregated data in the database if using PostgreSQL
            if self.db_type == 'postgresql':
                logger.info("Storing aggregated data in PostgreSQL data warehouse tables")
                self._store_aggregated_data_in_postgres(affiliate_sales, category_sales, monthly_sales)
            
            # Generate PDF report
            logger.info("Generating PDF report")
            self._generate_pdf_report(affiliate_sales, category_sales, monthly_sales, summary)
            
            logger.info("Report generation completed successfully")
            return True
        except Exception as e:
            logger.error(f"Error generating reports: {str(e)}")
            return False
    
    def _store_aggregated_data_in_postgres(self, affiliate_sales, category_sales, monthly_sales):
        """
        Store aggregated report data in PostgreSQL data warehouse tables.
        
        Args:
            affiliate_sales (pandas.DataFrame): DataFrame containing affiliate sales data
            category_sales (pandas.DataFrame): DataFrame containing category sales data
            monthly_sales (pandas.DataFrame): DataFrame containing monthly sales data
        
        Returns:
            bool: True if data was stored successfully, False otherwise
        """
        try:
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            with DatabaseTransaction(self.conn) as cursor:
                # Store affiliate sales data
                logger.info("Storing affiliate sales data in PostgreSQL")
                for _, row in affiliate_sales.iterrows():
                    cursor.execute(
                        """
                        INSERT INTO fact_affiliate_sales (affiliate_name, total_sales_usd, last_updated)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (affiliate_name) DO UPDATE
                        SET total_sales_usd = EXCLUDED.total_sales_usd,
                            last_updated = EXCLUDED.last_updated
                        """,
                        (row['affiliate_name'], row['total_sales_usd'], current_time)
                    )
                
                # Store category sales data
                logger.info("Storing category sales data in PostgreSQL")
                for _, row in category_sales.iterrows():
                    cursor.execute(
                        """
                        INSERT INTO fact_category_sales (category, total_sales_usd, last_updated)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (category) DO UPDATE
                        SET total_sales_usd = EXCLUDED.total_sales_usd,
                            last_updated = EXCLUDED.last_updated
                        """,
                        (row['category'], row['total_sales_usd'], current_time)
                    )
                
                # Store monthly sales data
                logger.info("Storing monthly sales data in PostgreSQL")
                for _, row in monthly_sales.iterrows():
                    cursor.execute(
                        """
                        INSERT INTO fact_monthly_sales (month, total_sales_usd, last_updated)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (month) DO UPDATE
                        SET total_sales_usd = EXCLUDED.total_sales_usd,
                            last_updated = EXCLUDED.last_updated
                        """,
                        (row['month'], row['total_sales_usd'], current_time)
                    )
            
            logger.info("Successfully stored all aggregated data in PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Error storing aggregated data in PostgreSQL: {str(e)}")
            return False
    
    def _generate_pdf_report(self, affiliate_sales, category_sales, monthly_sales, summary):
        """
        Generate a professional PDF report with visualizations.
        
        Args:
            affiliate_sales (pandas.DataFrame): DataFrame containing affiliate sales data
            category_sales (pandas.DataFrame): DataFrame containing category sales data
            monthly_sales (pandas.DataFrame): DataFrame containing monthly sales data
            summary (pandas.DataFrame): DataFrame containing summary statistics
        
        Returns:
            bool: True if PDF was generated successfully, False otherwise
        """
        try:
            # Set up the PDF
            pdf_path = f"{self.reports_dir}/sales_report.pdf"
            with PdfPages(pdf_path) as pdf:
                # Set style for better visualization
                plt.style.use('seaborn-v0_8-whitegrid')
                
                # Create a professional cover page
                plt.figure(figsize=(8.5, 11))
                plt.axis('off')
                
                # No border on the first page for a cleaner look
                
                # Add title and subtitle with better formatting
                plt.text(0.5, 0.85, 'SALES REPORT', fontsize=28, ha='center', weight='bold', color='#003366')
                plt.text(0.5, 0.78, 'Executive Summary', fontsize=18, ha='center', style='italic', color='#666666')
                plt.text(0.5, 0.72, f'Generated on {datetime.now().strftime("%B %d, %Y at %I:%M %p")}', 
                         fontsize=12, ha='center', color='#666666')
                
                # Add a horizontal separator line
                plt.axhline(y=0.68, xmin=0.1, xmax=0.9, color='#003366', linewidth=2)
                
                # Format summary statistics in a more professional way
                plt.text(0.5, 0.60, 'SUMMARY STATISTICS', fontsize=14, ha='center', weight='bold', color='#003366')
                
                # Create a formatted summary text with proper alignment and spacing
                total_orders = summary['total_orders'].values[0]
                total_sales = summary['total_sales_usd'].values[0]
                avg_order = summary['avg_order_value_usd'].values[0]
                min_order = summary['min_order_value_usd'].values[0]
                max_order = summary['max_order_value_usd'].values[0]
                
                # Create a table-like structure for summary statistics
                col1_x = 0.25
                col2_x = 0.75
                row_start = 0.55
                row_height = 0.05
                
                # Row 1
                plt.text(col1_x, row_start, 'Total Orders:', fontsize=12, ha='right', weight='bold')
                plt.text(col2_x, row_start, f"{total_orders:,}", fontsize=12, ha='left')
                
                # Row 2
                plt.text(col1_x, row_start-row_height, 'Total Sales (USD):', fontsize=12, ha='right', weight='bold')
                plt.text(col2_x, row_start-row_height, f"${total_sales:,.2f}", fontsize=12, ha='left')
                
                # Row 3
                plt.text(col1_x, row_start-2*row_height, 'Average Order Value:', fontsize=12, ha='right', weight='bold')
                plt.text(col2_x, row_start-2*row_height, f"${avg_order:,.2f}", fontsize=12, ha='left')
                
                # Row 4
                plt.text(col1_x, row_start-3*row_height, 'Minimum Order Value:', fontsize=12, ha='right', weight='bold')
                plt.text(col2_x, row_start-3*row_height, f"${min_order:,.2f}", fontsize=12, ha='left')
                
                # Row 5
                plt.text(col1_x, row_start-4*row_height, 'Maximum Order Value:', fontsize=12, ha='right', weight='bold')
                plt.text(col2_x, row_start-4*row_height, f"${max_order:,.2f}", fontsize=12, ha='left')
                
                # Add footer
                plt.text(0.5, 0.05, 'CONFIDENTIAL - FOR INTERNAL USE ONLY', fontsize=8, ha='center', color='#999999')
                plt.text(0.5, 0.03, 'ETL, Reporting & PDF Export Pipeline', fontsize=8, ha='center', color='#999999')
                
                # Add the cover page to the PDF
                pdf.savefig()
                plt.close()
                
                # Create charts page with better layout
                plt.figure(figsize=(8.5, 11))
                
                # Add page title
                plt.suptitle('Sales Performance Analysis', fontsize=16, y=0.98, weight='bold', color='#003366')
                
                # Create affiliate sales chart with improved styling
                plt.subplot(3, 1, 1)
                ax1 = sns.barplot(x='total_sales_usd', y='affiliate_name', hue='affiliate_name', data=affiliate_sales, palette='Blues_d', legend=False)
                plt.title('Sales by Affiliate (USD)', fontsize=12, pad=10)
                plt.xlabel('Total Sales (USD)', fontsize=10)
                plt.ylabel('Affiliate', fontsize=10)
                
                # Add value labels to the bars
                for i, v in enumerate(affiliate_sales['total_sales_usd']):
                    ax1.text(v + 5, i, f"${v:.2f}", va='center', fontsize=8)
                
                # Create category sales chart with improved styling
                plt.subplot(3, 1, 2)
                ax2 = sns.barplot(x='total_sales_usd', y='category', hue='category', data=category_sales, palette='Greens_d', legend=False)
                plt.title('Sales by Category (USD)', fontsize=12, pad=10)
                plt.xlabel('Total Sales (USD)', fontsize=10)
                plt.ylabel('Category', fontsize=10)
                
                # Add value labels to the bars
                for i, v in enumerate(category_sales['total_sales_usd']):
                    ax2.text(v + 5, i, f"${v:.2f}", va='center', fontsize=8)
                
                # Create monthly sales chart with improved styling
                plt.subplot(3, 1, 3)
                ax3 = sns.lineplot(x='month', y='total_sales_usd', data=monthly_sales, marker='o', color='#8B0000', linewidth=2)
                plt.title('Monthly Sales Trend (USD)', fontsize=12, pad=10)
                plt.xlabel('Month', fontsize=10)
                plt.ylabel('Total Sales (USD)', fontsize=10)
                plt.xticks(rotation=45)
                
                # Add value labels to the points
                for i, (x, y) in enumerate(zip(monthly_sales['month'], monthly_sales['total_sales_usd'])):
                    ax3.text(i, y + 50, f"${y:.2f}", ha='center', fontsize=8)
                
                plt.tight_layout(rect=[0, 0, 1, 0.96])  # Adjust layout to accommodate the title
                
                # Add the charts page to the PDF
                pdf.savefig()
                plt.close()
                
                # Create a more professional tables page
                # Affiliate sales table
                self._create_table_page(
                    pdf, 
                    'Sales by Affiliate', 
                    affiliate_sales, 
                    'This table shows the total sales amount in USD for each affiliate, sorted by highest sales.'
                )
                
                # Category sales table
                self._create_table_page(
                    pdf, 
                    'Sales by Category', 
                    category_sales, 
                    'This table shows the total sales amount in USD for each product category, sorted by highest sales.'
                )
                
                # Monthly sales table
                self._create_table_page(
                    pdf, 
                    'Monthly Sales Trend', 
                    monthly_sales, 
                    'This table shows the total sales amount in USD for each month, including unknown dates.'
                )
            
            logger.info(f"PDF report generated successfully at {pdf_path}")
            return True
        except Exception as e:
            logger.error(f"Error generating PDF report: {str(e)}")
            return False
    
    def _create_table_page(self, pdf, title, data, description):
        """
        Helper method to create a professional table page for the PDF report.
        
        Args:
            pdf: PdfPages object
            title (str): Title of the table page
            data (pandas.DataFrame): DataFrame containing the data to display
            description (str): Description of the table
        """
        plt.figure(figsize=(8.5, 11))
        plt.axis('off')
        
        # Add page title
        plt.text(0.5, 0.95, title, fontsize=16, ha='center', weight='bold', color='#003366')
        
        # Add description
        plt.text(0.5, 0.9, description, fontsize=10, ha='center', style='italic', color='#666666')
        
        # Format the data for the table
        # Round numeric values to 2 decimal places and add dollar signs
        formatted_data = data.copy()
        if 'total_sales_usd' in formatted_data.columns:
            formatted_data['total_sales_usd'] = formatted_data['total_sales_usd'].apply(lambda x: f"${x:.2f}")
        
        # Create the table with better formatting
        table_data = [formatted_data.columns.tolist()] + formatted_data.values.tolist()
        
        # Calculate column widths based on content
        col_widths = [0.5, 0.3]  # Default widths
        
        # Create and style the table
        table = plt.table(
            cellText=table_data,
            loc='center',
            cellLoc='center',
            colWidths=col_widths,
            bbox=[0.15, 0.3, 0.7, 0.5]  # [left, bottom, width, height]
        )
        
        # Style the table
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        
        # Style the header row
        for (i, j), cell in table.get_celld().items():
            if i == 0:  # Header row
                cell.set_text_props(weight='bold', color='white')
                cell.set_facecolor('#003366')
            else:  # Data rows
                if i % 2 == 0:  # Even rows
                    cell.set_facecolor('#f2f2f2')
                    
            # Add borders
            cell.set_edgecolor('#333333')
        
        # Add footer
        plt.text(0.5, 0.05, 'CONFIDENTIAL - FOR INTERNAL USE ONLY', fontsize=8, ha='center', color='#999999')
        plt.text(0.5, 0.03, f'Generated on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}', fontsize=8, ha='center', color='#999999')
        
        # Add the table page to the PDF
        pdf.savefig()
        plt.close()
