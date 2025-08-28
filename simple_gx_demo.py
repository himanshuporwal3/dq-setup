#!/usr/bin/env python3
"""
Simplified Great Expectations Demo
Demonstrates running data quality expectations on sample data
"""

import pandas as pd
import great_expectations as gx
from datetime import datetime
import json
import sys
import os

def create_sample_data():
    """Create sample data that matches our config (test1 and test2 tables)"""
    
    # Sample data for test1 table
    test1_data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice Johnson', 'Bob Smith', 'Carol Davis', 'David Wilson', 'Eve Brown'],
        'email': ['alice@example.com', 'bob@example.com', 'carol@example.com', 'david@example.com', 'eve@example.com'],
        'created_date': pd.to_datetime(['2024-01-15', '2024-02-20', '2024-03-10', '2024-04-05', '2024-05-12'])
    }
    
    # Sample data for test2 table  
    test2_data = {
        'product_id': [101, 102, 103, 104, 105],
        'product_name': ['Laptop Pro', 'Wireless Mouse', 'USB-C Cable', 'Monitor Stand', 'Keyboard'],
        'price': [1299.99, 49.99, 24.99, 89.99, 79.99],
        'category': ['Electronics', 'Electronics', 'Electronics', 'Electronics', 'Electronics']
    }
    
    return pd.DataFrame(test1_data), pd.DataFrame(test2_data)

def run_basic_expectations():
    """Run basic expectations using modern GX API"""
    
    print("🚀 Starting Great Expectations Demo")
    print("=" * 50)
    
    try:
        # Create sample data
        test1_df, test2_df = create_sample_data()
        print(f"✓ Created sample data - test1: {len(test1_df)} rows, test2: {len(test2_df)} rows")
        
        # Initialize GX context
        context = gx.get_context()
        print("✓ Initialized Great Expectations context")
        
        # Add pandas datasource
        datasource = context.sources.add_pandas("demo_datasource")
        print("✓ Added pandas datasource")
        
        # Add data assets
        test1_asset = datasource.add_dataframe_asset("test1", dataframe=test1_df)
        test2_asset = datasource.add_dataframe_asset("test2", dataframe=test2_df)
        print("✓ Added data assets for test1 and test2 tables")
        
        # Run expectations for test1
        print("\n📋 Running expectations for TEST1 table...")
        test1_validator = context.get_validator(
            batch_request=test1_asset.build_batch_request(),
            expectation_suite_name="test1_suite"
        )
        
        # Add expectations for test1
        result1 = test1_validator.expect_table_row_count_to_be_between(min_value=1, max_value=1000000)
        print(f"   Row count expectation: {'✓ PASS' if result1.success else '✗ FAIL'}")
        
        result2 = test1_validator.expect_column_values_to_not_be_null("id")
        print(f"   ID not null expectation: {'✓ PASS' if result2.success else '✗ FAIL'}")
        
        result3 = test1_validator.expect_column_values_to_be_unique("id")  
        print(f"   ID unique expectation: {'✓ PASS' if result3.success else '✗ FAIL'}")
        
        result4 = test1_validator.expect_column_values_to_not_be_null("name")
        print(f"   Name not null expectation: {'✓ PASS' if result4.success else '✗ FAIL'}")
        
        result5 = test1_validator.expect_column_values_to_match_regex("email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
        print(f"   Email format expectation: {'✓ PASS' if result5.success else '✗ FAIL'}")
        
        # Save test1 suite
        test1_validator.save_expectation_suite()
        print("✓ Saved test1 expectation suite")
        
        # Run expectations for test2
        print("\n📋 Running expectations for TEST2 table...")
        test2_validator = context.get_validator(
            batch_request=test2_asset.build_batch_request(),
            expectation_suite_name="test2_suite"
        )
        
        # Add expectations for test2
        result6 = test2_validator.expect_table_row_count_to_be_between(min_value=1, max_value=500000)
        print(f"   Row count expectation: {'✓ PASS' if result6.success else '✗ FAIL'}")
        
        result7 = test2_validator.expect_column_values_to_not_be_null("product_id")
        print(f"   Product ID not null expectation: {'✓ PASS' if result7.success else '✗ FAIL'}")
        
        result8 = test2_validator.expect_column_values_to_be_unique("product_id")
        print(f"   Product ID unique expectation: {'✓ PASS' if result8.success else '✗ FAIL'}")
        
        result9 = test2_validator.expect_column_values_to_not_be_null("product_name")
        print(f"   Product name not null expectation: {'✓ PASS' if result9.success else '✗ FAIL'}")
        
        result10 = test2_validator.expect_column_values_to_be_between("price", min_value=0, max_value=10000)
        print(f"   Price range expectation: {'✓ PASS' if result10.success else '✗ FAIL'}")
        
        result11 = test2_validator.expect_column_values_to_be_in_set("category", ["Electronics", "Clothing", "Books", "Home", "Sports"])
        print(f"   Category values expectation: {'✓ PASS' if result11.success else '✗ FAIL'}")
        
        # Save test2 suite
        test2_validator.save_expectation_suite()
        print("✓ Saved test2 expectation suite")
        
        # Create and run checkpoints
        print("\n🔍 Creating and running checkpoints...")
        
        # Test1 checkpoint
        test1_checkpoint = context.add_or_update_checkpoint(
            name="test1_checkpoint",
            batch_request=test1_asset.build_batch_request(),
            expectation_suite_name="test1_suite"
        )
        
        test1_results = test1_checkpoint.run()
        test1_success = test1_results["success"]
        print(f"   Test1 checkpoint: {'✓ PASS' if test1_success else '✗ FAIL'}")
        
        # Test2 checkpoint
        test2_checkpoint = context.add_or_update_checkpoint(
            name="test2_checkpoint", 
            batch_request=test2_asset.build_batch_request(),
            expectation_suite_name="test2_suite"
        )
        
        test2_results = test2_checkpoint.run()
        test2_success = test2_results["success"]
        print(f"   Test2 checkpoint: {'✓ PASS' if test2_success else '✗ FAIL'}")
        
        # Generate summary
        print("\n📊 VALIDATION SUMMARY")
        print("=" * 50)
        print(f"Tables validated: 2")
        print(f"Test1 result: {'✓ PASSED' if test1_success else '✗ FAILED'}")
        print(f"Test2 result: {'✓ PASSED' if test2_success else '✗ FAILED'}")
        print(f"Overall success: {'✓ ALL PASSED' if test1_success and test2_success else '⚠ SOME FAILED'}")
        
        # Try to build data docs
        try:
            context.build_data_docs()
            print("✓ Generated data documentation")
        except Exception as e:
            print(f"⚠ Could not generate data docs: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error running expectations demo: {e}")
        import traceback
        traceback.print_exc()
        return False

def show_sample_data():
    """Display the sample data being validated"""
    print("\n📊 SAMPLE DATA PREVIEW")
    print("=" * 50)
    
    test1_df, test2_df = create_sample_data()
    
    print("TEST1 Table (Users):")
    print(test1_df.to_string(index=False))
    
    print("\nTEST2 Table (Products):")
    print(test2_df.to_string(index=False))
    
    print(f"\nData types - TEST1:")
    for col, dtype in test1_df.dtypes.items():
        print(f"  {col}: {dtype}")
        
    print(f"\nData types - TEST2:")
    for col, dtype in test2_df.dtypes.items():
        print(f"  {col}: {dtype}")

if __name__ == "__main__":
    print("🎯 Great Expectations Data Quality Demo")
    print("Testing expectations on sample Databricks-style data")
    print("=" * 60)
    
    # Show sample data first
    show_sample_data()
    
    # Run the expectations demo
    success = run_basic_expectations()
    
    if success:
        print("\n🎉 Demo completed successfully!")
        print("All expectations have been tested and validated.")
    else:
        print("\n❌ Demo failed - check error messages above")
        sys.exit(1)