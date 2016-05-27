import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

public class Main {

    private static OptionParser.ParsedArgs parsed = null;
    private static HashMap<String,Object>  config = null;

    public static void out(String s)    { System.out.println(s); }
    public static void outs(String s)   { System.out.print(s); }
    public static void err(String s)    { out(s); System.exit(2); }

    public static void streamMetadata(ResultSet rs, OptionParser.ParsedArgs parsed) throws SQLException {
        String fieldSep = parsed.val("field-separator");
        boolean align = !parsed.is("dont-align");
        boolean rightPad = !parsed.is("right-align");
        boolean leave = !parsed.is("leave-whitespace");
        ResultSetMetaData md = rs.getMetaData();
        int rows = md.getColumnCount()+1;
        // put the metadata into memory
        String[] titles = "index\tlabel\tname\ttype#\ttype\twidth\tclass\tproperties (s=searchable,w=writable,r=readonly,n=nullable,o=nonnullable,^=case_sensitive,$=currency,+=auto_increment)".split("\t");
        int cols = titles.length;
        String[][] eles = new String[rows][cols];
        int[][] limits = int2DArray(0, 2, cols);  // row 0=max length seen, 1=absolute column width limit
        limits[1] = intArray(132,cols);

        // column titles @ zeroth row
        for (int i=0; i<cols; i++) {
            capture(limits,eles,0,i,titles[i]);
        }
        for(int i=1; i<rows; i++) {
            capture(limits,eles,i,0,""+i);
            capture(limits,eles,i,1,md.getColumnLabel(i));
            capture(limits,eles,i,2,md.getColumnName(i));
            capture(limits,eles,i,3,md.getColumnType(i));
            capture(limits,eles,i,4,md.getColumnTypeName(i));
            capture(limits,eles,i,5,md.getColumnDisplaySize(i));
            capture(limits,eles,i,6,md.getColumnClassName(i));
            capture(limits,eles,i,7,props(md, i));
        }
        flush(limits,eles,rows,align,rightPad,leave,fieldSep);
    }

    public static String props(ResultSetMetaData md, int i) throws SQLException {
        StringBuffer sb = new StringBuffer();
        if (md.isSearchable(i)) sb.append('s');
        else sb.append('_');
        if (md.isWritable(i)) sb.append('w');
        else sb.append('_');
        if (md.isReadOnly(i)) sb.append('r');
        else sb.append('_');
        if (md.isNullable(i) == ResultSetMetaData.columnNullable) sb.append('n');
        else if (md.isNullable(i) == ResultSetMetaData.columnNoNulls) sb.append('o');
        else sb.append('_');
        if (md.isCaseSensitive(i)) sb.append('^');
        else sb.append('_');
        if (md.isCurrency(i)) sb.append('$');
        else sb.append('_');
        if (md.isAutoIncrement(i)) sb.append('+');
        else sb.append('_');
        return sb.toString();
    }

    public static String blobstr(ResultSet rs, int i) throws SQLException {
        return String.format("(%dB blob)",(rs.getBlob(i)==null?0:rs.getBlob(i).length()));
    }

    private static String transformVal(String s) {
        return s.replaceAll("[\u0000-\u0009|\u000B|\u000e-\u001f]", "");
    }

    public static String transform(String s) {
        if (s == null)
            return "0T0+0?0";
        long start = System.currentTimeMillis();
        String r = transformVal(s);
        long stop = System.currentTimeMillis();
        return ""+(stop-start)+"T"+s.length()+"+"+r.length()+"?"+(r.length()-s.length());
    }

    // update the max length seen along with the storing the value itself
    public static void capture(int[][] limits, String[][] eles, int row, int column, Object val) {
        eles[row][column] = safestr(val);
        if (limits==null) return;
        limits[0][column] = Math.max(eles[row][column].length(), limits[0][column]);
    }

    public static int padAmt(String s, int n) {
        int len = s.length();
        if (len>n) return 0;
        return n-len;
    }

    private static char[] spaces;
    private static Pattern whiteout = Pattern.compile("\\s");

    public static String niceify(String s, int max, boolean leaveWhitespace) {
        s = leaveWhitespace ? s : whiteout.matcher(s).replaceAll(" ");
        if (s.length()<=max) return s;
        return s.substring(0,max-3)+"...";
    }

    // output the rows
    public static void flush(int[][] limits, String[][] eles, int rows, boolean align, boolean rightPad, boolean leaveWhitespace, String fieldSep) {
        StringBuffer sb = new StringBuffer();
        String rowSep = "";
        for(int j=0;j<rows;j++) {
            sb.append(rowSep);
            String[] row = eles[j];
            String colSep = "";
            for(int i=0;i<row.length;i++) {
                sb.append(colSep);
                if (align&&!rightPad) sb.append(spaces,0, padAmt(row[i], Math.min(limits[1][i],limits[0][i]))); // limits[0] is desired column width
                sb.append(niceify(row[i],limits[1][i],leaveWhitespace)); // limits[1] is max column width
                if (align&&rightPad) sb.append(spaces,0, padAmt(row[i], Math.min(limits[1][i],limits[0][i]))); // limits[0] is desired column width
                colSep = fieldSep;
            }
            rowSep = "\n";
        }
        out(sb.toString());
    }

    public static void streamResultMem(ResultSet rs, OptionParser.ParsedArgs parsed) throws SQLException
    {
        ResultSetMetaData md = rs.getMetaData();
        String fieldSep = parsed.val("field-separator");
        int[] col = columnsFor(parsed.has("columns") ? parsed.val("columns") : "1-" + md.getColumnCount());
        boolean[] applytx = txfor(col, columnsFor(parsed.has("transform") ? parsed.val("transform") : ""));
        boolean align = !parsed.is("dont-align");
        boolean rightPad = !parsed.is("right-align");
        boolean leave = parsed.is("leave-whitespace");
        int truncateAt = Integer.parseInt(parsed.val("limit"));
        int nAtAtime = Integer.parseInt(parsed.val("buffer"));

        // stream the result in memory for the first nAtAtime rows and determine 'desired' column width from them.
        String[][] eles = new String[nAtAtime][col.length];
        int[][] limits = int2DArray(0, 2, col.length);  // row 0=max length seen, 1=absolute column width limit
        limits[1] = intArray(truncateAt,col.length);
        int[][] findLimits = limits;

        // put the column titles in mem and capture more col info
        int row = 0;
        boolean[] isBlob = new boolean[col.length];
        for (int i=0; i<col.length; i++) {
            int c = col[i];
            isBlob[i] = md.getColumnTypeName(c).toUpperCase() == "BLOB";
            capture(findLimits,eles,row,i,md.getColumnName(c));
        }

        // now do the rows of the resultset
        int total = 0;
        row++;
        long ts = System.currentTimeMillis();
        while (rs.next()) {
            if(row>=nAtAtime) {
                flush(limits,eles,row,align,rightPad,leave,fieldSep);
                row = 0;
                findLimits = null; // stop looking for limits after the first flush
            }
            for(int i=0; i<col.length; i++) {
                int c = col[i];
                String s = isBlob[i] ? blobstr(rs, c) : rs.getString(c);
                s = applytx[i] ? transform(s) : s;
                capture(findLimits,eles,row,i,s);
            }
            total++;
            row++;
        }
        flush(limits,eles,row,align,rightPad,leave,fieldSep);
        long te = System.currentTimeMillis();
        long tdiff = te-ts;
        if (parsed.is("timed")) out(total+" accessed in "+tdiff+"ms");
    }

    public static int[] toIntArray(List<Integer> l) {
        int[] arr = new int[l.size()];
        int i = 0;
        for(int v : l)
            arr[i++] = v;
        return arr;
    }

    public static int[] columnsFor(String s) {
        List<Integer> a = new ArrayList<Integer>();
        String[] l = s.split(",",-1);
        for(int i=0; i<l.length; i++) {
            try {
                String[] range = l[i].split("-");
                if (range.length>1) {
                    int start = Integer.parseInt(range[0]);
                    int end = Integer.parseInt(range[1]);
                    if (end<start) err("Range should be from low to high values; bad range '"+l[i]+"' in argument '"+s+"'");
                    for(int j=start;j<=end;j++)
                        a.add(j);
                } else if (range[0].length()>0) {
                    a.add(Integer.parseInt(range[0]));
                }
            } catch (NumberFormatException e) {
                err("Not a proper number or range '"+l[i]+"' in argument '"+s+"'");
            }
        }
        return toIntArray(a);
    }

    public static Connection getConn() throws Exception
    {
        long ts = System.currentTimeMillis();
        String driver = parsed.val("jdbc");
        String dbURL = parsed.val("db");
        Properties p = new Properties();
        p.put("remarksReporting","true");  // oracle specific to support remarks (e.g. column comments)
        if (parsed.has("user")) p.put("user", parsed.val("user"));
        if (parsed.has("password")) p.put("password", parsed.val("password"));

        if (parsed.has("source") && config!=null) {
            try {
                Map src = (Map)config.get("sources");
                Map<String,String> s = (Map<String,String>)src.get(parsed.val("source"));
                for(Map.Entry<String,String> e : s.entrySet()) {
                    if (e.getKey().equals("url"))
                        dbURL = e.getValue();
                    else if (e.getKey().equals("driver"))
                        driver = e.getValue();
                    else
                        p.put(e.getKey(),e.getValue());
                }
            } catch(Exception e) {
                err("failed to find source '"+parsed.val("source")+"' in jdbcq.conf file");
            }
        }

        if (driver == null) driver = driverOf(dbURL);
        if (parsed.is("verbose"))
            out("connecting "+driver+" to "+dbURL+" with properties "+p.toString());

        Class.forName(driver).newInstance();
        Connection c = DriverManager.getConnection(dbURL, p);
        if (c!=null && (parsed.is("verbose") || parsed.is("interactive")))
            out(driverInfo(c.getMetaData()));
        if (parsed.is("timed")) {
            long te = System.currentTimeMillis();
            out("connection obtained in "+(te-ts)+"ms");
        }
        parsed.opts.put("db",dbURL);
        return c;
    }


    public static String driverOf(String url) {
        url = url.toLowerCase();
        String d = "oracle.jdbc.OracleDriver";  // default oracle
        if (url.contains("mysql")) {
            d = "com.mysql.jdbc.Driver";
        } else if (url.contains("postgresql")) {
            d = "org.postgresql.Driver";
        } else if (url.contains("sqlserver")) {
            d = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        } else if (url.contains("cassandra")) {
            d = "org.apache.cassandra.cql.jdbc.CassandraDriver";
        } else if (url.contains("mongo")) {
            d = "mongodb.jdbc.MongoDriver";
        }
        return d;
    }

    public static String driverInfo(DatabaseMetaData m) {
        StringBuilder sb = new StringBuilder();
        try { sb.append(m.getDriverName()); } catch(SQLException e) {}
        sb.append('/');
        try { sb.append(m.getDriverVersion()); } catch(SQLException e) {}
        sb.append(" -> ");
        try { sb.append(m.getDatabaseProductName() ); } catch(SQLException e) {}
        sb.append('/');
        try { sb.append(m.getDatabaseProductVersion() ); } catch(SQLException e) {}
        return sb.toString();
    }

    public static void loadConfig() {
        boolean foundit = false;
        File f = null;
        for(String fn : new String[]{"./jdbcq.conf", System.getProperty("user.home")+System.getProperty("file.separator")+".jdbcq.conf"}) {
            f = new File(fn);
            if (f.exists() && f.canRead()) {
                foundit = true;
                break;
            }
        }
        if (foundit) {
            try {
                Yaml yaml = new Yaml();
                config = (HashMap<String,Object>) yaml.load(new FileInputStream(f));
                if (parsed.is("verbose")) {
                    Map src = (Map)config.get("sources");
                    out("Found "+src.keySet().toString()+" data sources in config file '"+f.getCanonicalPath()+"'");
                }
            } catch (IOException e) { e.printStackTrace(); }
        } else if (parsed.is("verbose")) {
            try { out("no config file found at "+f.getCanonicalPath()); } catch(IOException e) {}
        }
    }

    public static String defaultQuery(Connection c, String ord, String tbl, int first, int end) {
        String o = "";
        try {
            String nm = c.getMetaData().getDriverName().toLowerCase();
            if (nm.contains("oracle")) {
                o = (ord == null) ? String.format("SELECT t.* FROM %s t WHERE ROWNUM BETWEEN %d AND %d", tbl,first,end)
                                  : String.format("SELECT t.* FROM ( SELECT ROW_NUMBER() OVER (ORDER BY %s) AS rn, t.* FROM %s t ) t WHERE rn BETWEEN %d AND %d", ord,tbl,first,end);
            } else if (nm.contains("mysql")) {
                o = (ord == null) ? String.format("SELECT * FROM %s LIMIT %d,%d", tbl,first-1,end-first)
                                  : String.format("SELECT * FROM %s ORDER BY %s LIMIT %d,%d", tbl,ord,first-1,end-first);
            } else {
                o = (ord == null) ? String.format("SELECT * FROM %s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", tbl,first-1,end-first)
                                  : String.format("SELECT * FROM %s ORDER BY %s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", tbl,ord,first-1,end-first);
            }
        } catch(SQLException e) {}
        return o;
    }

    public static String readFile(String fn) throws FileNotFoundException {
        return new Scanner(new File(fn)).useDelimiter("\\Z").next();
    }

    public static String join(String sep, List l) { return join(sep,l.iterator());}

    public static String join(String sep, Iterator l) {
        StringBuilder sb = new StringBuilder();
        String asep = "";
        while(l.hasNext()) {
            Object o = l.next();
            if (o!=null) {
                sb.append(asep);
                sb.append(o.toString());
                asep = sep;
            }
        }
        return sb.toString();
    }

    public static String pkToString(ResultSet rs) throws SQLException {
        String[] ar = new String[20];
        while(rs.next()) {
            String val = rs.getString(4);
            int i = rs.getInt(5);
            ar[i] = val;
        }
        return join(",",Arrays.asList(ar));
    }


    public static String fkToString(ResultSet rs) throws SQLException {
        String[] ar = new String[20];
        while(rs.next()) {
            String pktbl = rs.getString(3);
            String pkcol = rs.getString(4);
            String fktbl = rs.getString(7);
            String fkcol = rs.getString(8);
            int i = rs.getInt(9);
            ar[i] = String.format("%s.%s->%s",fktbl,fkcol,pkcol);
        }
        return join(",",Arrays.asList(ar));
    }


    public static String ikToString(ResultSet rs) throws SQLException {
        String[] ar = new String[20];
        while(rs.next()) {
            String pktbl = rs.getString(3);
            String pkcol = rs.getString(4);
            String fktbl = rs.getString(7);
            String fkcol = rs.getString(8);
            int i = rs.getInt(9);
            ar[i] = String.format("%s.%s->%s",pktbl,pkcol,fkcol);
        }
        return join(",",Arrays.asList(ar));
    }

    public static String idxtype2str(int v) {
        String s = "unk";
        if (v==DatabaseMetaData.tableIndexStatistic)
            s = "statistic";
        else if (v==DatabaseMetaData.tableIndexClustered)
            s = "cluster";
        else if (v==DatabaseMetaData.tableIndexHashed)
            s = "hash";
        else if (v==DatabaseMetaData.tableIndexOther)
            s = "other";
        return s;
    }

    public static String safestr(Object o) {
        return (o==null) ? "" : o.toString();
    }

    public static String idxToString(ResultSet rs) throws SQLException {
        int[] cols = columnsFor("1-"+rs.getMetaData().getColumnCount());
        //if (true) return resultString(rs,cols,null,999999);
        String[] ar = new String[20];
        int i = 0;
        while(rs.next()) {
            String nm = safestr(rs.getString(6));
            int type = rs.getShort(7);
            int pos = rs.getShort(8);
            String col = safestr(rs.getString(9));
            String asc = safestr(rs.getString(10));
            int card = rs.getInt(11);
            int pages = rs.getInt(12);
            if (nm.equals(""))
                continue;
            ar[i++] = String.format("%s[%s:%s:%s:%d:%d:%d]",col.toLowerCase(),nm.toLowerCase(),idxtype2str(type),asc.equals("A")?"asc":"dsc",pos,card,pages);
        }
        return join(",",Arrays.asList(ar));
    }

    public static boolean[] txfor(int[] columns, int[] tx) {
        boolean [] applytx = new boolean[columns.length];
        for(int t : tx) {
            applytx[t-1] = true;
        }
        return applytx;
    }

    public static void main(String[] args)
    {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            OptionParser o = new OptionParser();
            o.add_desc("Usage: jdbcq [-tn] [-c col_spec] [-q sql-query] table order-by [start [end]]\n\n" +
                    "Executes a 'select *' from the specified table returning the first 10 rows. If provided,\n" +
                    "'start' determines the starting row_number().  'end' can be used to indicate the terminating\n" +
                    "row_number().  By default all columns are output unless the '-c' option is used.  Columns\n" +
                    "are identified by their ordinal number and may be specified via a range (e.g. 5-8) or\n" +
                    "or individually, separated by a comma (e.g. 11,5-8,2).  The '-t' is used to time the\n" +
                    "query and subsequent access of the results without displaying them.  The default query\n" +
                    "can be overridden with the '-q' option, in which case table,order-by,start and end arguments\n" +
                    "are ignored.  Displaying only the meta-data (no results) associated with the query\n" +
                    "is achieved by using the '-n' option.  Quoting order-by and appending DESC will result\n" +
                    "in a descending sort order. E.g. jdbcq productsubmissions \"submissionid DESC\" -c 1,2");
            o.add_option('a', "dont-align", null, Boolean.class, "disable column alignment.");
            o.add_option('b', "buffer", "1000", String.class, "buffer this many rows before displaying.");
            o.add_option('c', "columns", null, String.class, "column numbers to display.");
            o.add_option('d', "db", null, String.class, "db url connection string");
            o.add_option('f', "field-separator", "\t", String.class, "character(s) to use to separate each field.");
            o.add_option('g', "right-align", null, Boolean.class, "align column text to the right.");
            o.add_option('i', "interactive", null, Boolean.class, "run in repl mode.");
            o.add_option('j', "jdbc", null, String.class, "use a specific jdbc driver.");
            o.add_option('k', "catalog", null, String.class, "specify a particular catalog name for schema queries");
            o.add_option('l', "limit", "100", String.class, "maximum number of characters to display per field.");
            o.add_option('m', "metadata", null, Boolean.class, "db metadata mode.");
            o.add_option('n', "dry-run", null, Boolean.class, "run the sql and display query metadata.");
            o.add_option('p', "password", "user", String.class, "password for the db login.");
            o.add_option('q', "query", null, String.class, "execute the given SQL instead of the default.");
            o.add_option('r', "1-row", null, Boolean.class, "fetch one row at a time; careful locks will *not* be released until entire statement is complete!");
            o.add_option('s', "source", null, String.class, "name of source to use (loaded from jdbcq.conf file)");
            o.add_option('t', "timed", null, Boolean.class, "time the access without displaying the results.");
            o.add_option('u', "user", "user", String.class, "user name for the db login.");
            o.add_option('v', "verbose", null, Boolean.class, "output informational messages.");
            o.add_option('w', "leave-whitespace", "false", Boolean.class, "dont filter whitespace from output.");
            o.add_option('x', "transform", null, String.class, "apply transform on a columns' field values; output is of the form timeToriginal_size+transformed_size?size_delta");
            o.add_option('y', "upper", "false", Boolean.class, "treat metadata requests only in uppercase.");
            try {
                parsed = o.parse_args(args);
            } catch(OptionParser.OptException e) {
                out("Bad option; use -h for help");
                System.exit(1);
            }
            spaces = charArray(' ',1000);
            loadConfig();
            if (parsed.is("interactive")) {
                repl();
                return;
            }
            if (parsed.has("metadata")) {
                // special case for db metadata traversal; no args = catalogs, 1 arg = table list, 2 args columns list
                conn = getConn();
                long ts = System.currentTimeMillis();
                StringBuilder extra = new StringBuilder();
                String norm = join(".",Arrays.asList(parsed.args));
                norm = parsed.is("upper") ? norm.toUpperCase() : norm;
                String[] spec = norm.split("\\.");
                String catalog = parsed.val("catalog");
                if (parsed.is("verbose")) out("metadata access for "+spec);
                if (spec.length==0 || spec[0].length()<1) {
                    boolean hasCat = conn.getMetaData().supportsCatalogsInTableDefinitions();
                    if (hasCat)
                        rs = conn.getMetaData().getCatalogs();
                    else
                        rs = conn.getMetaData().getSchemas();
                } else if (spec.length==1) {
                    rs = conn.getMetaData().getTables(catalog, spec[0], null, null);
                } else if (spec.length>1) {
                    rs = conn.getMetaData().getColumns(catalog, spec[0], spec[1], null);
                    extra.append("keys (primary) ");
                    extra.append(pkToString(conn.getMetaData().getPrimaryKeys(catalog,spec[0],spec[1])));
                    extra.append(" (export) ");
                    extra.append(fkToString(conn.getMetaData().getExportedKeys(catalog,spec[0],spec[1])));
                    extra.append(" (import) ");
                    extra.append(ikToString(conn.getMetaData().getImportedKeys(catalog,spec[0],spec[1])));
                    extra.append("\nindexes ");
                    extra.append(idxToString(conn.getMetaData().getIndexInfo(catalog,spec[0],spec[1],false,true)));
                }
                if (parsed.is("timed")) {
                    long te = System.currentTimeMillis();
                    out("metadata obtained in "+(te-ts)+"ms");
                }
                streamResultMem(rs, parsed);
                if (extra.length()>0) out(extra.toString());
            } else {
                //if (parsed.count()<2 && !parsed.has("query")) err("Need at least 2 arguments; table and order-by (-h or --help for help)");
                String tbl = parsed.arg(0);
                String ord = parsed.count() < 2 ? null : parsed.arg(1);
                int first  = parsed.count() < 3 ? 1 : Integer.parseInt(parsed.arg(2));
                int end    = parsed.count() < 4 ? first+10 : Integer.parseInt(parsed.arg(3));
                conn = getConn();
                String sql = parsed.has("query") ? parsed.val("query")
                                                 : defaultQuery(conn,ord,tbl,first,end);
                sql = sql.startsWith("@") ? readFile(sql.substring(1)) : sql;
                if (parsed.is("dry-run")) out(sql);
                long ts = System.currentTimeMillis();
                stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                if (parsed.is("1-row")) stmt.setFetchSize(Integer.MIN_VALUE);
                executeSQL(stmt,sql,ts,parsed);
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(3);
        } finally {
            if (rs != null)     { try { rs.close(); }   catch(Exception e) {} }
            if (stmt != null)   { try { stmt.close(); } catch(Exception e) {} }
            if (conn != null)   { try { conn.close(); } catch(Exception e) {} }
        }
    }

    public static void help() {
        out("bind    - connect to a database; optionally override <user> <passwd>\n" +
            "count   - select count(*) of table 'tbl'\n" +
            "db      - set/display database url for connection (used to override source)\n" +
            "display - select 10 rows from table 'tbl' or specify 'tbl order-by [start [end]]'\n" +
            "exec    - execute commands contained in file 'fn'\n" +
            "show    - display various metadata (type 'show' for more info)\n" +
            "use     - prepend a schema/catalog 'spec' to commands. 'use ;' resets it.\n" +
            "source  - .jdbc.conf file source name for the connection\n\n" +
            "The following commands display/set or toggle various options\n" +
            "   columns         - set/clear which columns to output\n" +
            "   dont-align      - toggle column alignment on and off\n" +
            "   dry-run         - display metadata of sql execution or the results\n" +
            "   field-separator - field separator string\n" +
            "   limit           - column width limit\n" +
            "   right-align     - toggle for left/right alignment\n" +
            "   timed           - show timing information\n" +
            "   upper           - convert 'show' names to uppercase\n" +
            "   verbose         - show details on what is happening\n" +
            "   whitespace      - toggle the removal of whitespace from output\n" +
            "");
    }

    private static void exec(Stack<BufferedReader> s, String fn) throws IOException { s.push(new BufferedReader(new InputStreamReader(Files.newInputStream(FileSystems.getDefault().getPath(fn))))); }

    private static BufferedReader currentInp(Stack<BufferedReader> s) {
        if (s.empty()) s.push(new BufferedReader(new InputStreamReader(System.in)));
        return s.peek();
    }

    private static String line(Stack<BufferedReader> s) throws Exception {
        String line=null;
        while(line==null) {
            if (!currentInp(s).ready() && s.size()>1) s.pop();
            if (s.size()<=1) outs("jdbcq> ");
            line = currentInp(s).readLine();
            if (line!=null && (line.trim().startsWith("#") || line.trim().startsWith("--"))) line=null;
            if (line!=null && line.trim().length()==0) line=null;
            if (line==null) Thread.sleep(100);
        }
        return line;
    }

    public static void repl() throws Exception {
        Connection conn = connect(new String[]{});
        Stack<BufferedReader> instack = new Stack<BufferedReader>();
        parsed.opts.put("use","");  // default '' for use command
        boolean done = false;
        while(!done) {
            String[] cmd = line(instack).split(" ");
            String sql = null;
            if (match("bind",1,cmd)) {
                conn = connect(cmd);
            } else if (match("columns",3,cmd)) {
                if (cmd.length>1) {parsed.opts.put("columns", cmd[1]);} else {parsed.opts.remove("columns");}
                out(parsed.has("columns") ? "columns " + parsed.val("columns") : "all columns displayed");
            } else if (match("count",3,cmd)) {
                if (cmd.length>1) { sql="select count(*) from "+cmd[1];}
            } else if (match("db",2,cmd)) {
                if (cmd.length>1) { parsed.opts.put("db",cmd[1]); parsed.opts.remove("source");}
                out("db is " + parsed.val("db"));
            } else if (match("dont-align",10,cmd)) {
                option("dont-align", cmd, parsed);
            } else if (match("dry-run",7,cmd)) {
                option("dry-run", cmd, parsed);
            } else if (match("exec",4,cmd)) {
                exec(instack, cmd[1]);
            } else if (match("field-separator",9,cmd)) {
                optionset("field-separator", cmd, parsed);
            } else if (match("help",4,cmd) || cmd[0]=="?") {
                help();
            } else if (match("limit",5,cmd)) {
                optionset("limit", cmd, parsed);
            } else if (match("quit",1,cmd)) {
                done = true;
            } else if (match("right-align",11,cmd)) {
                option("right-align", cmd, parsed);
            } else if (match("show",2,cmd)) {
                sql = show(conn, cmd);
            } else if (match("sc", 2, cmd)) { // show table alias
                show(conn, new String[]{"show","table",cmd.length>1?cmd[1]:null,cmd.length>2?cmd[2]:null});
            } else if (match("st", 2, cmd)) { // show tables alias
                show(conn, new String[]{"show", "tables", cmd.length > 1 ? cmd[1] : null, cmd.length > 2 ? cmd[2] : null});
            } else if (match("source", 6, cmd)) {
                optionset("source", cmd, parsed);
            } else if (match("timed",5,cmd)) {
                option("timed",cmd,parsed);
            } else if (match("upper",5,cmd)) {
                option("upper", cmd, parsed);
            } else if (match("use", 3, cmd)) {
                optionset("use", cmd, ".", parsed);
            } else if (match("verbose",7,cmd)) {
                option("verbose", cmd, parsed);
            } else if (match("whitespace",10,cmd)) {
                option("leave-whitespace", cmd, parsed);
            } else if (match("display",3,cmd)) {
                if (cmd.length<2) {
                    out("display command requires at least a table name");
                } else {
                    String tbl = parsed.val("use")+cmd[1];
                    String ord = cmd.length < 3 ? null : cmd[2];
                    int first  = cmd.length < 4 ? 1 : Integer.parseInt(cmd[3]);
                    int end    = cmd.length < 5 ? first+10 : Integer.parseInt(cmd[4]);
                    sql = defaultQuery(conn,ord,tbl,first,end);
                }
            } else {
                sql = join(" ", Arrays.asList(cmd));
            }
            // exec a sql statement if one was created.
            if (sql!=null) {
                long ts = System.currentTimeMillis();
                Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                if (parsed.is("1-row")) stmt.setFetchSize(Integer.MIN_VALUE);
                try { executeSQL(stmt,sql,ts,parsed);} catch (SQLException e) {out(exceptionSuggestions(e));}
            }
        }
    }

    private static String exceptionSuggestions(SQLException e) {
        String s = e.toString();
        if (s.endsWith("Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp"))
            s+="\nPossible remedy is to add ?zeroDateTimeBehavior=convertToNull to your source url";
        return s;
    }

    private static ResultSet showTables(Connection conn, String[] cmd) throws SQLException {
        ResultSet rs = null;
        String[] spec = dotspec(cmd, 2, parsed.is("upper"), parsed.val("use"));
        boolean hasCat = conn.getMetaData().supportsCatalogsInTableDefinitions();
        if (spec.length>1)
            rs = getTables(conn, spec[0], spec[1]);
        else if (hasCat && spec.length>0)
            rs = getTables(conn, spec[0], null);
        else if (spec.length>0)
            rs = getTables(conn,null,spec[0]);
        else
            rs = getTables(conn, null, null); //who supports this?
        return rs;
    }

    private static ResultSet showColumns(Connection conn, String[] cmd, StringBuffer extras) throws SQLException {
        ResultSet rs = null;
        if (cmd.length<3) { out("a table name must be specified"); return null; }
        String[] spec = dotspec(cmd, 2, parsed.is("upper"), parsed.val("use"));
        boolean hasCat = conn.getMetaData().supportsCatalogsInTableDefinitions();
        if (hasCat && spec.length>1)
            rs = getColumns(conn, spec[0], null, spec[1], extras);
        else if (spec.length>1)
            rs = getColumns(conn, null, spec[0], spec[1], extras);
        else if (spec.length>2)
            rs = getColumns(conn, spec[0], spec[1], spec[2], extras);
        return rs;
    }

    private static String show(Connection conn, String[] cmd) {
        if (cmd.length < 2) {
            out("show what? 'properties', 'types', 'catalogs', 'schemas', 'tables <sch>' or 'table <tbl>'");
            return null;
        }
        try {
            StringBuffer extras = new StringBuffer();
            long ts = System.currentTimeMillis();
            ResultSet rs = null;
            if (match("catalogs",8,cmd,1)) {
                rs = conn.getMetaData().getCatalogs();
            } else if (match("properties",10,cmd,1)) {
                rs = conn.getMetaData().getClientInfoProperties();
            } else if (match("schemas",7,cmd,1)) {
                rs = conn.getMetaData().getSchemas();
            } else if (match("types",5,cmd,1)) {
                rs = conn.getMetaData().getTypeInfo();
            } else if (match("tables",6,cmd,1)) {
                rs = showTables(conn, cmd);
            } else if (match("table",5,cmd,1)) {
                rs = showColumns(conn,cmd,extras);
            } else {
                return join(" ", Arrays.asList(cmd));
            }
            if (parsed.is("timed")) {
                long te = System.currentTimeMillis();
                out("metadata obtained in " + (te - ts) + "ms");
            }
            if (rs!=null) {
                int[] columns = columnsFor(parsed.has("columns") ? parsed.val("columns") : "1-"+rs.getMetaData().getColumnCount());
                streamResultMem(rs, parsed);
                if (extras.length()>0) out(extras.toString());
            }
        } catch(SQLException sql) {
            out("error while obtaining metadata - "+sql.getMessage());
        }
        return null;
    }

    public static String[] dotspec(String[] arr, int startAt, boolean upper, String use) {
        String norm = join(".", Arrays.asList(arr).listIterator(startAt));
        norm = upper ? (use+norm).toUpperCase() : use+norm;
        return norm.split("\\.");
    }

    public static ResultSet getColumns(Connection conn, String catalog, String schema, String table, StringBuffer extra) throws SQLException {
        ResultSet rs = conn.getMetaData().getColumns(catalog,schema,table,null);
        keyIndexExtras(conn,catalog,schema,table,extra);
        return rs;
    }

    public static ResultSet getTables(Connection conn, String catalog, String schema) throws SQLException {
        ResultSet rs = conn.getMetaData().getTables(catalog,schema,null,null);
        return rs;
    }

    public static void keyIndexExtras(Connection conn, String catalog, String schema, String table, StringBuffer extra) throws SQLException {
        extra.append("keys (primary) ");
        extra.append(pkToString(conn.getMetaData().getPrimaryKeys(catalog,schema,table)));
        extra.append(" (export) ");
        extra.append(fkToString(conn.getMetaData().getExportedKeys(catalog,schema,table)));
        extra.append(" (import) ");
        extra.append(ikToString(conn.getMetaData().getImportedKeys(catalog,schema,table)));
        extra.append("\nindexes ");
        extra.append(idxToString(conn.getMetaData().getIndexInfo(catalog,schema,table,false,true)));
    }

    public static boolean match(String what, int min,String[] cmd)          { return match(what,min,cmd,0); }
    public static boolean match(String what, int min, String[] cmd, int ele) { return cmd[ele].startsWith(what.substring(0,min)); }

    public static void option(String which, String[] cmd, OptionParser.ParsedArgs parsed) {
        boolean on = true;
        if (cmd.length>1) {
            on = cmd[1].startsWith("on") || cmd[1].startsWith("1");
        }
        parsed.opts.put(which,on?"true":"false");
        out(which+" is " + (on ? "on" : "off"));
    }

    public static void optionset(String which, String[] cmd, OptionParser.ParsedArgs parsed) { optionset(which,cmd,"",parsed); }

    public static void optionset(String which, String[] cmd, String append, OptionParser.ParsedArgs parsed) {
        try {
            String val = parsed.val(which);
            if (cmd.length>1) {
                val = cmd[1].equals(";") ? "" : cmd[1]+append; // ; => empty value
            }
            parsed.opts.put(which,val);
            out(which+" is " + val);
        } catch(Exception e) { out("failed to set "+which+" - "+e.toString()); }
    }


    private static Connection connect(String[] cmd) throws Exception {
        if (cmd.length>1) parsed.opts.put("user",cmd[1]);
        if (cmd.length>2) parsed.opts.put("password",cmd[2]);
        Connection conn=null;
        try { conn = getConn(); } catch(Exception e) {out("failed to connect to db - "+e.toString()); if (parsed.is("verbose")) e.printStackTrace(); }
        return conn;
    }

    private static void executeSQL(Statement stmt, String sql, long ts, OptionParser.ParsedArgs parsed) throws SQLException {
        if (parsed.is("verbose")) out("executing sql "+sql);
        boolean isRS = stmt.execute(sql);
        long te = System.currentTimeMillis();
        if (parsed.is("timed")) out("sql processed in "+(te-ts)+"ms");
        boolean done = false;
        while(!done) {
            if (isRS) {
                if (parsed.is("dry-run")) {
                    streamMetadata(stmt.getResultSet(), parsed);
                } else {
                    streamResultMem(stmt.getResultSet(),parsed);
                }
            } else {
                int cnt = stmt.getUpdateCount();
                if (cnt > -1) out("update count is " + cnt);
                done = true;
            }
            isRS = stmt.getMoreResults();
        }
    }

    public static int[] intArray(int val, int n) {
        int[] arr = new int[n];
        for(int i=0;i<arr.length;i++) { arr[i]=val; }
        return arr;
    }

    public static char[] charArray(char c, int n) {
        char[] arr = new char[n];
        for(int i=0;i<arr.length;i++) { arr[i]=c; }
        return arr;
    }

    public static int[][] int2DArray(int val, int n, int m) {
        int[][] arr = new int[n][m];
        for(int i=0;i<arr.length;i++) { arr[i]=intArray(val,m); }
        return arr;
    }
}
