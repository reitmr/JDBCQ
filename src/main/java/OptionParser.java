import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: rreitmaier
 * Date: 3/31/13
 * Time: 22:14
 * To change this template use File | Settings | File Templates.
 */
public class OptionParser {
    public class OptException extends Exception { OptException(String s) { super(s); } }

    class Dict extends HashMap<String,String> {}

    public class ParsedArgs {
        public Dict     opts;
        public String[] args;

        public ParsedArgs() { opts = new Dict(); }

        public String   val(String s)     { return opts.get(s); }
        public boolean  has(String s)     { return val(s)!=null; }
        public boolean  is(String s)      { return has(s) && val(s).equals("true"); }
        public String   arg(int i)        { return i<args.length ? args[i] : null; }
        public int      count()           { return args.length; }
        public int      valInt(String s)  { return Integer.parseInt(val(s)); }
    }

    class OptDef {
        char opt;
        String longopt;
        String help;
        String defaultVal;
        Class setClass;

        OptDef (char opt, String longopt, String defaultVal, Class setClass, String help) {
            this.opt = opt;
            this.longopt = longopt;
            this.help = help;
            this.defaultVal = defaultVal;
            this.setClass = setClass;
        }
    }

    String desc;
    List<OptDef> order;
    HashMap<String,OptDef> defs;

    public OptionParser() {
        defs = new HashMap<String,OptDef>();
        order = new ArrayList<OptDef>();
        add_option('h', "help", null, Boolean.class, "show this message and exit");
    }

    void out(String s)  { System.out.println(s);}
    void err(int rc)    { help(); System.exit(rc);}

    public void help() {
        out(desc);
        out("\nOptions:");
        for(OptDef o : order) {
            String s = o.setClass == Boolean.class || o.defaultVal==null
                    ? String.format("-%c, --%s\t\t\t%s",o.opt,o.longopt,o.help)
                    : String.format("-%c, --%s {value}\t\t%s (default %s)",o.opt,o.longopt,o.help,o.defaultVal);
            out(s);
        }
    }

    public void add_desc(String s) { desc=s; }

    public void add_option(char opt, String longopt, String defaultVal, Class valueClass, String helpMsg) {
        OptDef d = new OptDef(opt,longopt,defaultVal,valueClass, helpMsg);
        defs.put(String.valueOf(opt),d);
        defs.put(longopt,d);
        order.add(d);
    }

    public ParsedArgs parse_args(String[] a) throws OptException {
        ParsedArgs p = new ParsedArgs();
        for(OptDef o : order)
            p.opts.put(o.longopt,o.defaultVal);
        List<String> ordered = new ArrayList<String>();
        for(int i=0,cnt=a.length; i<cnt; i++) {
            String arg = a[i];
            int j = 0;
            boolean isbreak = false;
            int max = arg.length();
            char c = arg.charAt(j++);
            if (c != '-')
                ordered.add(arg);
            else {
                while(c!=' ' && j<max && !isbreak) {
                    c = arg.charAt(j++);
                    String lu = null;
                    if (c == '-') {
                        lu = arg.substring(2);
                        isbreak = true;
                    } else {
                        lu = String.valueOf(c);
                    }

                    OptDef hit = defs.get(lu);
                    if (hit == null) {
                        throw new OptException("'"+c+"' option not found");
                    } else if (hit.setClass == Boolean.class) {
                        p.opts.put(hit.longopt, "true");
                    } else if (i>=cnt) {
                            throw new OptException("expected space followed by value after '-"+c+" option, not"+arg.charAt(j));
                    } else {
                        arg = a[++i];
                        p.opts.put(hit.longopt, arg);
                        isbreak = true;
                    }
                }
            }
        }
        p.args = ordered.toArray(new String[]{});
        if (p.has("help")) err(2);
        return p;
    }
}
