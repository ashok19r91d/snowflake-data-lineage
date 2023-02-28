# Name of the script.
# =====================
#     Data Lineage Generator - Using Query History
# 
# Purpose of this script.
# =========================
#     This script is used to identify Data Lineage using Query History
#
# Developed by: Ashok Ramanan Dhatchinamoorthy (https://github.com/ashok19r91d)
#
# Strategy.
# ===========
#   1. Read DML Queries from Query History of Snowflake Metadata Views
#   2. Parse Query
#   3. Identify Source and Target tables for Each Query
#   4. Remove Duplicates
#   5. Consolidate Source tables for each target table (Remove Deleted Table, View and Stage references, Remove Duplicates, Trim, Build Fully Qualified Object Names)
#   6. Build JSON
#   7. Build dotGraph Notation for D3 Visual
#   8. Save HTML file and Check output
#
#
# ANSI Standard:
# ===============
#     1. SQL statements are not case sensitive contains Line Comments, Block Comments and Text values.
#     2. SELECT query can be mingled with all other statement types as Sub Query.
#     3. VIEW Definition start only after "AS" clause, but AS is also used to define alias of Table and Column name
#     4. A Single view can wrapped with multiple SELECT queries (Using SET operators, Sub Queries, CTEs)
#     5. Table name can be used in only few CLAUSES of ANSI Compliant SQL statements,
#        it can be found only (atleast this script designed to work) in following clasues,
#         A. INSERT INTO "TableName"
#         B. UPDATE "TableName"
#         C. DELETE FROM "TableName"
#         D. SELECT ... FROM "TableName" ... JOIN "TableName" ...
#         E. COPY INTO "TableName" FROM "TableName" (Either one is table name other is Stage name or path)
#         F. MERGE INTO "TableName" USING "TableName"
#     6. Collectively, Tablename follows just after following keywords,
#         A. FROM
#         B. JOIN
#         C. INTO
#         D. USING
#         E. UPDATE
#     7. TableNames always ends either before a White Space (includes tabs and new lines) or Closing rounded brackets or Semi colon.
#     8. At times, Instead of TableNames JOIN and FROM clause contains subquery (which internally relates one or more tablenames).
#     9. Queries with CTE can use CTE Name instead of TableName at some clauses described above
#    10. MERGE statement internally contains UPDATE keyword, is should not be confused with `UPDATE` statement. this UPDATE statement dosen't follow tablename instead it followed by `set` keyword, we can ignore this UPDATE while building table statement.
#    11. Tablenames may not used as fully qualified format, There should be additional work required to build Fully Qualified Object names
#    12. There are cases specially with COPY command, We may have Stage Name and Location instead of table name, That will starts with `@`.
#    13. For stage name or location, `@` should be first character of Fully Qualified Table name.
#    14. Any table names without double quote enclosement have to be considerd as UPPER case when building Fully qualified table name.
#
# How to Run this Script?
# ========================
# 1. Create Environment for Python
#    python -m venv env  (env is the environment name)
# 2. Activate your environment
#    \env\Scripts\activate
# 3. Install Dependency
#    pip install snowflake-connector-pyhton
#    pip install sqlparse
# 4. Update Password (along with other connection detail) for Snowflake connection, When possible it's advised to execute the script with ACCOUNTADMIN privileges. 
# 5. Run Script
#    python app.py
#

import datetime
import sqlparse
import snowflake.connector


def connect_to_snowflake():
    return snowflake.connector.connect(
        account='',
        user='',
        password='',
        warehouse=''
    )


def parse_query(query):
    return sqlparse.parse(query)[0]


def fully_qualified_object_name(db, sch, obj):
    # Rule 11
    if obj.startswith('@'):
        # Rule 12, 13
        obj = obj.strip('@')
        obj = f'@{db}.{obj}' if len(obj.split('.')) == 2 else (
            f"@{db}.{sch}.{obj}" if len(obj.split('.')) == 1 else f"@{obj}")
        parts = []
        for part in obj.split('.'):
            if not part.startswith('"'):
                # Rule 14
                part = part.upper()
            parts.append('"' + part.strip('"') + '"')
        return ".".join(parts)
    else:
        obj = f"{db}.{obj}" if len(obj.split('.')) == 2 else (
            f"{db}.{sch}.{obj}" if len(obj.split('.')) == 1 else obj)
        parts = []
        for part in obj.split('.'):
            if not part.startswith('"'):
                # Rule 14
                part = part.upper()
            parts.append('"' + part.strip('"') + '"')
        return ".".join(parts)


def build_dependency_mapping(tokens, writting_table_name, partial_table_name, target_table, source_tables, cte_identified, parent, db, sch, qt):
    cte_used = False
    for token in tokens:
        if type(token) == sqlparse.sql.Comment:
            # Rule 1
            pass
        elif token.is_keyword and token.normalized == 'SET':
            # Rule 10
            writting_table_name = False
            partial_table_name = ''
        elif cte_used and token.is_keyword and token.normalized == 'RECURSIVE':
            # Rule 9
            pass
        elif token.is_group:
            if cte_used:
                # Rule 9
                for tkn in token.tokens:
                    if tkn.is_group == False:
                        cte_identified.append(tkn.value.upper())
                    elif tkn.ttype == None:
                        if tkn.tokens[0].is_group:
                            cte_identified.append(
                                tkn.tokens[0].tokens[0].value.upper())
                        else:
                            cte_identified.append(tkn.tokens[0].value.upper())
            cte_used = False
            table_name_parser_result = None
            if (token.value.startswith('(')):
                # Rule 8
                table_name_parser_result = build_dependency_mapping(
                    token.tokens, False, '', target_table, source_tables, cte_identified, token.parent, db, sch, qt)
            else:
                table_name_parser_result = build_dependency_mapping(
                    token.tokens, writting_table_name, partial_table_name, target_table, source_tables, cte_identified, token.parent, db, sch, qt)

            partial_table_name = table_name_parser_result['Table Name']
            writting_table_name = table_name_parser_result['Still Writting']
            target_table = table_name_parser_result['Target Table']
            source_tables = table_name_parser_result['Source Tables']
        elif token.is_whitespace:
            # Rule 7
            if (writting_table_name and partial_table_name != ''):
                if partial_table_name == 'AS':
                    pass
                if partial_table_name.find(',') != -1:
                    pass
                IsCteName = False
                for cte in cte_identified:
                    if partial_table_name.upper() == cte:
                        IsCteName = True
                if IsCteName == False:
                    # Rule 9
                    if partial_table_name != '':
                        if target_table == '':
                            target_table = fully_qualified_object_name(
                                db, sch, partial_table_name)
                        else:
                            source_tables.append(fully_qualified_object_name(
                                db, sch, partial_table_name))
                writting_table_name = False
        else:
            if token.normalized == 'WITH':
                # Rule 9
                cte_used = True

            if writting_table_name and token.value != ')' and token.value != ';':
                # Rule 7
                partial_table_name += token.value

            if (token.normalized == 'TABLE' or token.normalized == 'FROM' or token.normalized == 'INTO' or token.normalized == 'UPDATE' or token.normalized == "USING" or (token.normalized.find(' JOIN') != -1 and token.ttype != None)):
                # Rule 5 and 6
                writting_table_name = True
                partial_table_name = ''

            if ((token.value == ')' or token.value == ';') and writting_table_name):
                # Rule 7
                IsCteName = False
                for cte in cte_identified:
                    if partial_table_name == cte:
                        IsCteName = True
                if IsCteName == False:
                    # Rule 9
                    if partial_table_name != '':
                        if target_table == '':
                            target_table = fully_qualified_object_name(
                                db, sch, partial_table_name)
                        else:
                            source_tables.append(fully_qualified_object_name(
                                db, sch, partial_table_name))
                writting_table_name = False
    if (parent == None):
        if source_tables.count(partial_table_name) == 0:
            if partial_table_name != '':
                if target_table == '':
                    target_table = fully_qualified_object_name(
                        db, sch, partial_table_name)
                else:
                    source_tables.append(fully_qualified_object_name(
                        db, sch, partial_table_name))
        return {'Target Table': target_table, 'Source Tables': list(set(source_tables)), 'Start Time': qt}
    else:
        return {'Target Table': target_table, 'Source Tables': source_tables, 'Table Name': partial_table_name, 'Still Writting': writting_table_name}


def fetch_query_history():
    Connection = connect_to_snowflake()
    sql = "select DATABASE_NAME, SCHEMA_NAME, MAX(START_TIME) AS QUERY_TIME, QUERY_TEXT from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY where EXECUTION_STATUS = 'SUCCESS' and QUERY_TYPE in ('MERGE', 'COPY', 'CREATE_TABLE_AS_SELECT', 'INSERT', 'UNLOAD', 'UPDATE') group by 1, 2, 4"
    query_history = Connection.cursor().execute(sql)
    for db, sch, time, qry in query_history:
        yield (db, sch, time, qry)


def fetch_tables_and_stages():
    Connection = connect_to_snowflake()
    sql = """select TABLE_CATALOG as DATABASE, TABLE_SCHEMA as SCHEMA, TABLE_NAME as OBJECT, 'Table' AS TYPE from SNOWFLAKE.ACCOUNT_USAGE.TABLES where TABLE_CATALOG = 'EDW_DEV' and DELETED is null
union all
select STAGE_CATALOG as DATABASE, STAGE_SCHEMA as SCHEMA, STAGE_NAME as OBJECT, 'Stage' as TYPE from SNOWFLAKE.ACCOUNT_USAGE.STAGES where STAGE_CATALOG = 'EDW_DEV' and DELETED is null
    """
    objects = []
    for db, sch, obj, typ in Connection.cursor().execute(sql):
        objects.append(fully_qualified_object_name(
            db, sch, f'{ obj }' if typ == 'Table' else f'@{ obj }'))
    return objects


def complete_mapping(query, db, sch, qt):
    return build_dependency_mapping(parse_query(query), False, '', '', [], [], None, db, sch, qt)


def identify_data_lineage():
    for db, sch, qt, qry in fetch_query_history():
        yield complete_mapping(qry, db, sch, qt)


def lineageJSON():
    source_target_mapping = {}
    for lineage in identify_data_lineage():
        if source_target_mapping.get(lineage['Target Table']) == None:
            source_target_mapping[lineage['Target Table']] = {'Source Tables': [], 'Start Time': datetime.datetime(
                2012, 11, 1, 4, 16, 13, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=72000)))}
        if source_target_mapping[lineage['Target Table']]['Start Time'] < lineage['Start Time']:
            if len(lineage['Source Tables']) != 0:
                source_target_mapping[lineage['Target Table']
                                      ]['Source Tables'] = lineage['Source Tables']
                source_target_mapping[lineage['Target Table']
                                      ]['Start Time'] = lineage['Start Time']

    target_tables = list(source_target_mapping.keys())
    target_tables.sort()
    for target_table in target_tables:
        yield {target_table: source_target_mapping[target_table]['Source Tables']}


def drawNode(node, objs):
    if (node.startswith('@') or node.startswith('"@')):
        for obj in objs:
            if node.startswith(obj.rstrip('"')):
                return '"@' + node.split(".")[2].strip('"') + "\\nSchema: " + node.split('.')[1].strip('"') + '"'
    else:
        if node in objs:
            return '"' + node.split(".")[2].strip('"') + "\\nSchema: " + node.split('.')[1].strip('"') + '"'
    return None


def getDot(objects):
    nodes = []
    edges = ""
    objs = fetch_tables_and_stages()
    for node in lineageJSON():
        for target in node.keys():
            target_node = drawNode(target, objs)
            if target_node is None:
                pass
            else:
                if target not in objects:
                    objects.append(target)
                    nodes.append(target_node)
                    # nodes += f"  {target_node};\n"
                for source in node[target]:
                    source_node = drawNode(source, objs)
                    if source_node is None:
                        pass
                    else:
                        if source not in objects:
                            objects.append(source)
                            nodes.append(source_node)
                            # nodes += f"  {source_node};\n"
                        edges += f'  { source_node } -> { target_node } [ style="solid" ];\n'
    notation = 'digraph G {\n\n'
    notation += '  graph [ rankdir="LR" bgcolor="#ffffff" ]\n'
    obj_group = {
        "EXTERNAL": [],
        "STAGING": [],
        "RAW": [],
        "DIM": [],
        "FACT": [],
        "AGG": [],
        "OTHERS": []
    }
    for obj in nodes:
        if obj.startswith('"@'):
            obj_group['EXTERNAL'].append(obj)
        elif obj.endswith('STAGING"'):
            obj_group['STAGING'].append(obj)
        elif obj.endswith('RAW"'):
            obj_group['RAW'].append(obj)
        elif obj.endswith('CORE"') and obj.startswith('"DIM_'):
            obj_group['DIM'].append(obj)
        elif obj.endswith('CORE"') and obj.startswith('"FACT_'):
            obj_group['FACT'].append(obj)
        elif obj.endswith('CORE"') and obj.startswith('"AGG_'):
            obj_group['AGG'].append(obj)
        else:
            obj_group['OTHERS'].append(obj)

    # S3 Stage
    notation += '  node [ style="filled" shape="record" color="#D7CCC8" ]\n'
    for obj in obj_group['EXTERNAL']:
        notation += f'  { obj };\n'
    # STAGING
    notation += '  node [ style="filled" shape="record" color="#FFECB3" ]\n'
    for obj in obj_group['STAGING']:
        notation += f'  { obj };\n'
    # RAW
    notation += '  node [ style="filled" shape="record" color="#DCEDC8" ]\n'
    for obj in obj_group['RAW']:
        notation += f'  { obj };\n'
    # CORE.DIM
    notation += '  node [ style="filled" shape="record" color="#B2EBF2" ]\n'
    for obj in obj_group['DIM']:
        notation += f'  { obj };\n'
    # CORE.FACT
    notation += '  node [ style="filled" shape="record" color="#C5CAE9" ]\n'
    for obj in obj_group['FACT']:
        notation += f'  { obj };\n'
    # CORE.AGG
    notation += '  node [ style="filled" shape="record" color="#E1BEE7" ]\n'
    for obj in obj_group['AGG']:
        notation += f'  { obj };\n'
    # OTHERS
    notation += '  node [ style="filled" shape="record" color="#B388FF" ]\n'
    for obj in obj_group['OTHERS']:
        notation += f'  { obj };\n'

    notation += f'  edge [ penwidth="2" color="#696969" dir="forward" ]\n\n'
    notation += f'{edges}'
    notation += '}'
    return notation


def save_file(dot_expr):
    s = '''<!DOCTYPE html>
        <meta charset="utf-8">
        <body>
        <script src="https://d3js.org/d3.v5.min.js"></script>
        <script src="https://unpkg.com/@hpcc-js/wasm@0.3.11/dist/index.min.js"></script>
        <script src="https://unpkg.com/d3-graphviz@3.0.5/build/d3-graphviz.js"></script>
        <div id="graph" style="text-align: center;"></div>
        <script>
        var graphviz = d3.select("#graph").graphviz()
           .on("initEnd", () => { graphviz.renderDot(d3.select("#digraph").text()); });
        </script>
        <div id="digraph" style="display:none;">''' + dot_expr + '</div>'
    with open('lineage.html', 'w') as file:
        file.write(s)


def rewrite_lineage_table():
    data = []
    for node in lineageJSON():
        for target in node.keys():
            target_node = target
            for source in node[target]:
                source_node = source
                data.append((target_node, source_node))
    snowflake.connector.paramstyle = 'qmark'
    # connect_to_snowflake().cursor().executemany("insert into EDW_DEV.UTIL.DATA_LINEAGE (TARGET_TABLE, SOURCE_TABLE) values (?, ?)", data)
    pass


save_file(getDot([]))

# rewrite_lineage_table()
