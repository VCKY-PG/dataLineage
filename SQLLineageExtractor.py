import re


class SQLLineageExtractor:
    def __init__(self):
        # Initialize storage for lineage records (optional, for accumulated usage)
        self.lineage_records = []


    def _extract_ctes(self, query):
        """Extract CTE subqueries and return (main_query_without_CTEs, cte_lineage_list)."""
        cte_lineage = []
        q = query.strip()
        q_lower = q.lower()
        if not q_lower.startswith('with '):
            return q, cte_lineage
        idx = q_lower.index('with ') + len('with ')
        # Loop through CTE definitions
        while True:
            # Find the CTE name and its subquery (assumes "name AS (...)")
            match = re.match(r'(\w+)\s+as\s*\(', q_lower[idx:])
            if not match:
                break
            cte_name = match.group(1)
            # Determine the span of the subquery within parentheses
            start_subq = q_lower.find('(', idx + match.end(1) - 1)
            if start_subq == -1:
                break
            open_brackets = 0
            end_subq = -1
            for j in range(start_subq, len(q)):
                if q[j] == '(':
                    open_brackets += 1
                elif q[j] == ')':
                    open_brackets -= 1
                    if open_brackets == 0:
                        end_subq = j
                        break
            if end_subq == -1:
                break  # unbalanced parentheses
            subquery_sql = q[start_subq+1:end_subq]
            # Recursively extract lineage of the CTE's subquery
            sub_lineage = self.extract_lineage(subquery_sql)
            # Determine sources feeding this CTE (exclude intermediate targets)
            sources = set()
            for entry in sub_lineage:
                sources.update(entry['sources'])
            for entry in sub_lineage:
                if entry['target'] in sources:
                    sources.remove(entry['target'])
            cte_lineage.extend(sub_lineage)
            cte_lineage.append({'target': cte_name, 'sources': sorted(sources)})
            # Move index past this CTE definition (skip comma if present)
            idx = end_subq + 1
            while idx < len(q_lower) and q_lower[idx].isspace():
                idx += 1
            if idx < len(q_lower) and q_lower[idx] == ',':
                idx += 1
                continue  # next CTE
            else:
                # End of CTE section; remaining query is the main query after CTEs
                main_query = q[idx:].strip()
                return main_query, cte_lineage
        # If we exit loop without hitting a break, return the query as is
        return q, cte_lineage


    def _extract_subqueries(self, query):
        """Replace subqueries in FROM/JOIN with an alias and return (query_with_aliases, subquery_lineage_list)."""
        sub_lineage = []
        q = query
        q_lower = q.lower()
        search_idx = 0
        # Find subqueries of the form "(SELECT ... ) alias" in FROM/JOIN clauses
        while True:
            from_pos = q_lower.find(' from (', search_idx)
            join_pos = q_lower.find(' join (', search_idx)
            pos = from_pos if (from_pos != -1 and (join_pos == -1 or from_pos < join_pos)) else join_pos
            if pos == -1:
                break
            # Identify the subquery block
            start_subq = q_lower.find('(', pos)
            if start_subq == -1 or not q_lower[start_subq+1:].strip().startswith('select'):
                search_idx = start_subq + 1 if start_subq != -1 else pos + 1
                continue
            # Find matching closing parenthesis for the subquery
            open_brackets = 0
            end_subq = -1
            for j in range(start_subq, len(q)):
                if q[j] == '(':
                    open_brackets += 1
                elif q[j] == ')':
                    open_brackets -= 1
                    if open_brackets == 0:
                        end_subq = j
                        break
            if end_subq == -1:
                break
            subquery_sql = q[start_subq+1:end_subq]
            # Get alias after the subquery
            alias_start = end_subq + 1
            while alias_start < len(q) and q[alias_start].isspace():
                alias_start += 1
            alias_end = alias_start
            while alias_end < len(q) and (q[alias_end].isalnum() or q[alias_end] == '_'):
                alias_end += 1
            alias = q[alias_start:alias_end]
            # Recursively get lineage of the subquery
            inner_lineage = self.extract_lineage(subquery_sql)
            sources = set()
            for entry in inner_lineage:
                sources.update(entry['sources'])
            for entry in inner_lineage:
                if entry['target'] in sources:
                    sources.remove(entry['target'])
            sub_lineage.extend(inner_lineage)
            sub_lineage.append({'target': alias, 'sources': sorted(sources)})
            # Replace the subquery with its alias in the query string
            q = q[:start_subq] + alias + q[alias_end:]
            q_lower = q.lower()
            # Continue searching after this alias
            search_idx = q_lower.find(alias.lower(), start_subq) + len(alias)
        return q, sub_lineage


    def _extract_tables_from_from_clause(self, query):
        """Extract base table names from the FROM/JOIN section of the query."""
        tables = []
        q_lower = query.lower()
        from_idx = q_lower.find(' from ')
        if from_idx == -1:
            return tables
        # Determine end of FROM... section by finding the first occurrence of common clauses after it
        end_idx = len(query)
        for kw in [' where ', ' group by ', ' having ', ' order by ', ' union ', ' limit ']:
            kw_pos = q_lower.find(kw, from_idx)
            if kw_pos != -1 and kw_pos < end_idx:
                end_idx = kw_pos
        from_section = query[from_idx:end_idx]
        # Remove join conditions (after "ON") and parenthesized content for simpler parsing
        from_section = re.sub(r'(?i) on [^,]*', '', from_section)
        from_section = re.sub(r'\([^)]*\)', '', from_section)
        # Find table names after FROM, JOIN, or commas
        for match in re.finditer(r'(?:from|join|,)\s+([^,\s]+)', from_section, flags=re.IGNORECASE):
            name = match.group(1).strip('`"')
            if name and name.lower() not in ('select','on','using','inner','outer','left','right','full','cross','join'):
                if name not in tables:
                    tables.append(name)
        return tables


    def _extract_columns_lineage(self, query, sources):
        """Extract column-level lineage (target_column -> source_column mapping) from SELECT clause."""
        columns_lineage = []
        q_lower = query.lower()
        select_idx = q_lower.find('select')
        if select_idx == -1:
            return columns_lineage
        # Isolate the SELECT list (between SELECT and FROM ... or end of query)
        from_idx = q_lower.find(' from ', select_idx)
        select_list = query[select_idx+len('select '): from_idx if from_idx != -1 else None]
        # Split select_list by commas at top level (not inside parentheses)
        exprs = []
        open_brackets = 0
        current = ''
        for ch in select_list:
            if ch == '(':
                open_brackets += 1
            elif ch == ')':
                open_brackets -= 1
            elif ch == ',' and open_brackets == 0:
                exprs.append(current.strip())
                current = ''
                continue
            current += ch
        if current.strip():
            exprs.append(current.strip())
        # Process each expression for alias and source reference
        for expr in exprs:
            if not expr:
                continue
            alias = None
            source_col = None
            # Check for "... AS alias" or implicit alias (last token)
            m = re.search(r'\s+as\s+(\w+)$', expr, flags=re.IGNORECASE)
            if m:
                alias = m.group(1)
                expr_core = expr[:m.start()].strip()
            else:
                parts = expr.split()
                if len(parts) > 1:
                    alias = parts[-1]
                    expr_core = ' '.join(parts[:-1])
                else:
                    alias = parts[0]
                    expr_core = parts[0]
            # Identify a single source column reference in the expression
            col_refs = re.findall(r'(\w+\.\w+)', expr_core)
            if len(col_refs) == 1:
                source_col = col_refs[0]
            elif len(col_refs) == 0 and '.' not in expr_core:
                if len(sources) == 1:
                    source_col = f"{sources[0]}.{expr_core}"
            if alias and source_col:
                columns_lineage.append({'target_column': alias, 'source_column': source_col})
        return columns_lineage


    def extract_lineage(self, sql):
        """Parse the SQL and return a list of lineage records (each a dict with 'target' and 'sources', plus 'columns' if available)."""
        # Remove comments and trailing semicolon
        query = re.sub(r'--.*', '', sql)
        query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL).strip().rstrip(';')
        if not query:
            return []
        q_lower = query.lower()
        lineage = []
        # Handle CREATE TABLE/VIEW (DDL)
        if q_lower.startswith('create'):
            m = re.match(r'create\s+(?:or\s+replace\s+)?(?:table|view)\s+([^\s(]+)', q_lower)
            target_name = m.group(1) if m else None
            as_idx = q_lower.find(' as ')
            if target_name and as_idx != -1:
                inner_query = query[as_idx+4:].strip()
                # Remove wrapping parentheses if present
                if inner_query.startswith('(') and inner_query.endswith(')'):
                    inner_query = inner_query[1:-1].strip()
                inner_lineage = self.extract_lineage(inner_query)
                # Gather all sources from inner lineage that are not intermediate
                sources = set()
                for entry in inner_lineage:
                    sources.update(entry['sources'])
                for entry in inner_lineage:
                    if entry['target'] in sources:
                        sources.remove(entry['target'])
                target_entry = {'target': target_name, 'sources': sorted(sources)}
                # Include column mapping for view creation if applicable
                if inner_query.lower().startswith('select'):
                    cols_lineage = self._extract_columns_lineage(inner_query, sorted(sources))
                    if cols_lineage:
                        target_entry['columns'] = cols_lineage
                lineage.extend(inner_lineage)
                lineage.append(target_entry)
                return lineage
        # Handle INSERT ... SELECT (DML)
        if q_lower.startswith('insert'):
            m = re.match(r'insert\s+into\s+([^\s(]+)', q_lower)
            target_name = m.group(1) if m else None
            select_idx = q_lower.find('select')
            if target_name and select_idx != -1:
                select_query = query[select_idx:]
                sub_lineage = self.extract_lineage(select_query)
                sources = set()
                for entry in sub_lineage:
                    sources.update(entry['sources'])
                for entry in sub_lineage:
                    if entry['target'] in sources:
                        sources.remove(entry['target'])
                target_entry = {'target': target_name, 'sources': sorted(sources)}
                lineage.extend(sub_lineage)
                lineage.append(target_entry)
                return lineage
        # Handle CTEs in the query
        main_query, cte_lineage = self._extract_ctes(query)
        # Handle subqueries in FROM/JOIN clauses
        processed_query, sub_lineage = self._extract_subqueries(main_query)
        # Extract base tables from the final processed query
        base_sources = self._extract_tables_from_from_clause(processed_query)
        # Combine all lineage parts
        lineage.extend(cte_lineage)
        lineage.extend(sub_lineage)
        # (If this is a standalone SELECT query, we don't append a 'target' entry for it here. 
        # The calling context like a CTE, insert, or create will handle the target.)
        # Optionally, we can capture column lineage for this query (for analysis or parent usage)
        if base_sources:
            cols_lineage = self._extract_columns_lineage(processed_query, base_sources)
            if cols_lineage:
                lineage.append({'target': '(columns)', 'sources': [], 'columns': cols_lineage})
        return lineage
# Initialize the extractor
extractor = SQLLineageExtractor()


# Example query with a CTE and join
query = """
WITH sales_data AS (
   SELECT s.sales_id, s.amount, c.region 
   FROM raw_sales s 
   JOIN customers c ON s.cust_id = c.id
)
INSERT INTO report_table 
SELECT region, SUM(amount) AS total_sales 
FROM sales_data 
GROUP BY region
"""
lineage = extractor.extract_lineage(query)


# Convert lineage result to JSON for API output
import json
print(json.dumps(lineage, indent=2))
