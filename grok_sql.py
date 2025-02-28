import re
import json
import sys

def parse_sql(query):
    schema = {}       # {table_name: [columns]}
    lineage = []      # [(source, target, condition)] - all dependencies
    aliases = {}      # {alias: table_name}

    # Normalize query
    query = " ".join(query.split()).lower()
    print("Normalized query:", query)

    # Parse table and alias
    def parse_table_alias(table_text):
        parts = table_text.split()
        table = parts[0]
        alias = parts[1] if len(parts) > 1 else table
        aliases[alias] = table
        return table, alias

    # Extract FROM clause and lineage
    def extract_from_clause(from_text, target_table, context=""):
        print(f"Processing FROM in {context} for target {target_table}: {from_text}")
        # Match table [alias] and optional JOINs
        pattern = r'([a-z_]\w*(?:\s+[a-z_]\w*)?)(?:\s*(left\s+)?join\s+([a-z_]\w*(?:\s+[a-z_]\w*)?)\s+on\s+(.+?)(?=\s*(?:left\s+)?join|where|group by|order by|$))?'
        matches = re.finditer(pattern, from_text, re.DOTALL)
        prev_alias = None
        for match in matches:
            main_table_text = match.group(1)
            join_table_text = match.group(3)
            condition = match.group(4)
            if main_table_text:
                main_table, main_alias = parse_table_alias(main_table_text)
                if main_table not in schema:
                    schema[main_table] = []
                lineage.append((main_table, target_table, "FROM"))  # Implicit lineage
                prev_alias = main_alias
            if join_table_text and condition:
                join_table, join_alias = parse_table_alias(join_table_text)
                if join_table not in schema:
                    schema[join_table] = []
                lineage.append((prev_alias, join_alias, condition.strip()))  # Explicit join
                print(f"Lineage detected: {prev_alias} -> {join_alias} ON {condition.strip()}")
                prev_alias = join_alias
        if not list(matches):  # Single table case
            table_text = from_text.strip()
            table, alias = parse_table_alias(table_text)
            if table not in schema:
                schema[table] = []
            lineage.append((table, target_table, "FROM"))
            print(f"Lineage detected: {table} -> {target_table} (FROM)")

    # Parse CTEs
    cte_blocks = []
    cte_start = query.find('with')
    if cte_start != -1:
        cte_end = query.rfind(')') + 1
        cte_section = query[cte_start:cte_end]
        print("CTE section:", cte_section)
        cte_pattern = r'([a-z_]\w*)\s+as\s+\((.+?)\)(?=\s*,|\s*select|$)'  # Match each CTE
        cte_matches = re.finditer(cte_pattern, cte_section, re.DOTALL)
        for match in cte_matches:
            cte_name = match.group(1).strip()
            cte_body = match.group(2).strip()
            cte_blocks.append((cte_name, cte_body))

    for cte_name, cte_body in cte_blocks:
        print(f"Parsing CTE: {cte_name}")
        select_match = re.search(r'select\s+(.+?)\s+from', cte_body, re.DOTALL)
        if select_match:
            columns = [col.strip() for col in select_match.group(1).split(',')]
            columns = [re.sub(r'[\(\):]', '', col.split(' as ')[0].split('.')[-1]).strip() for col in columns]
            schema[cte_name] = columns
        from_match = re.search(r'from\s+(.+?)(?=\s*(where|group by|order by|$))', cte_body, re.DOTALL)
        if from_match:
            extract_from_clause(from_match.group(1), cte_name, f"CTE {cte_name}")

    # Parse main query
    main_query = query.split('select', 1)[-1]
    print("Main query:", main_query)
    select_match = re.search(r'select\s+(.+?)\s+from', main_query, re.DOTALL)
    if select_match:
        columns = [col.strip() for col in select_match.group(1).split(',')]
        columns = [re.sub(r'[\(\):]', '', col.split(' as ')[0]).strip() for col in columns]

    from_match = re.search(r'from\s+(.+?)(?=\s*(where|group by|order by|$))', main_query, re.DOTALL)
    if from_match:
        extract_from_clause(from_match.group(1), "main_query", "main query")
        for col in columns:
            if '.' in col:
                alias, col_name = col.split('.', 1)
                table = aliases.get(alias, alias)
                if table not in schema:
                    schema[table] = []
                if col_name not in schema[table]:
                    schema[table].append(col_name)

    return schema, lineage, aliases

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <sql_file_path>")
        sys.exit(1)

    sql_file_path = sys.argv[1]
    try:
        with open(sql_file_path, 'r') as file:
            sql_query = file.read().strip()
    except FileNotFoundError:
        print(f"Error: File '{sql_file_path}' not found.")
        sys.exit(1)

    print(f"Parsing SQL from file '{sql_file_path}':\n{sql_query}")
    schema, lineage, aliases = parse_sql(sql_query)

    print("\nData Model (Schema):")
    for table, columns in schema.items():
        print(f"{table}: {columns}")
    
    print("\nLineage:")
    for link in lineage:
        print(f"{link[0]} -> {link[1]} ({link[2]})")

    print("\nAliases:")
    for alias, table in aliases.items():
        print(f"{alias}: {table}")

    data = {"tables": schema, "lineage": lineage, "aliases": aliases}
    with open("sql_data.json", "w") as f:
        json.dump(data, f, indent=2)
    print("\nSaved to sql_data.json")
