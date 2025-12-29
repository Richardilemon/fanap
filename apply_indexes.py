"""
Apply database indexes for performance optimization
"""
import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

def apply_indexes():
    """Read and execute the indexes SQL file"""
    
    # Get database URL from environment
    database_url = os.getenv('DATABASE_URL')
    
    if not database_url:
        print("❌ DATABASE_URL not found in environment")
        print("   Make sure it's set in your .env file")
        return False
    
    print("=" * 70)
    print("🔧 APPLYING DATABASE INDEXES")
    print("=" * 70)
    print(f"\n📍 Database: {database_url.split('@')[1] if '@' in database_url else 'Unknown'}\n")
    
    try:
        # Read SQL file
        sql_file = 'sql/indexes.sql'
        
        if not os.path.exists(sql_file):
            print(f"❌ SQL file not found: {sql_file}")
            return False
        
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        print(f"📄 Reading {sql_file}...")
        
        # Connect to database
        conn = psycopg2.connect(database_url, sslmode='require')
        cursor = conn.cursor()
        
        print("✅ Connected to database\n")
        print("🔨 Creating indexes...\n")
        
        # Execute SQL (split by semicolon for better error reporting)
        statements = [s.strip() for s in sql_content.split(';') if s.strip()]
        
        created = 0
        skipped = 0
        errors = 0
        
        for statement in statements:
            # Skip comments and empty statements
            if not statement or statement.startswith('--'):
                continue
            
            try:
                cursor.execute(statement)
                
                # Check what was executed
                if 'CREATE INDEX' in statement.upper():
                    # Extract index name
                    index_name = statement.split('IF NOT EXISTS')[1].split('ON')[0].strip() if 'IF NOT EXISTS' in statement else 'unknown'
                    print(f"  ✅ Created/verified: {index_name}")
                    created += 1
                elif 'ANALYZE' in statement.upper():
                    table_name = statement.replace('ANALYZE', '').strip()
                    print(f"  📊 Analyzed: {table_name}")
                elif 'SELECT' in statement.upper():
                    # This is the verification query at the end
                    print("\n📋 Verifying indexes:")
                    results = cursor.fetchall()
                    
                    print(f"\n{'Table':<25} {'Index Name':<40} {'Status'}")
                    print("-" * 70)
                    
                    for row in results:
                        schema, table, index, definition = row
                        print(f"{table:<25} {index:<40} ✅")
                
            except psycopg2.Error as e:
                # Index already exists or other error
                if "already exists" in str(e):
                    skipped += 1
                else:
                    print(f"  ⚠️  Error: {e}")
                    errors += 1
                conn.rollback()
                continue
        
        # Commit changes
        conn.commit()
        
        print("\n" + "=" * 70)
        print("📊 SUMMARY")
        print("=" * 70)
        print(f"  Indexes created/verified: {created}")
        if skipped > 0:
            print(f"  Skipped (already exist): {skipped}")
        if errors > 0:
            print(f"  Errors: {errors}")
        print("\n✅ Database indexes applied successfully!")
        print("=" * 70)
        
        cursor.close()
        conn.close()
        
        return True
        
    except psycopg2.Error as e:
        print(f"\n❌ Database error: {e}")
        return False
    except FileNotFoundError as e:
        print(f"\n❌ File error: {e}")
        return False
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        return False


if __name__ == '__main__':
    import sys
    success = apply_indexes()
    sys.exit(0 if success else 1)