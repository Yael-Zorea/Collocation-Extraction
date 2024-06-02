import com.amazonaws.regions.Regions;

public class Defns {

    public static final String TAB = "\t";
    public static final String first = " "; // '\0'

    public enum ValueType {N, Cw1w2, Cw1, Cw2, Collab, Npmi, SumNpmis, NpmiWithVal}

    public static Regions regions = Regions.US_EAST_1;
    public static String placementRegion = "us-east-1a"; 

    public static final int instanceCount = 9;

    public static final String HADOOP_VER = "3.3.6";
    public static final String KEY_NAME_SSH = "keyPair";

    public static final String TERMINATE_JOB_FLOW_MESSAGE = "TERMINATE_JOB_FLOW";

    public static final String PROJECT_NAME = "collocations-extraction";
    public static final String JAR_NAME = "CollocationsExtractionJar";
    public static final String JAR_PATH = "s3://" + PROJECT_NAME + "/" + JAR_NAME + ".jar";
    public static final String Logs_URI = "s3://" + PROJECT_NAME + "/logs";

    public static final String ENG_2Gram_path = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data";
    public static final String small_ENG_2Gram_path = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-1M/2gram/data";
    public static final String HEB_2Gram_path = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";

    public static final String[] Steps_Names = {"StepCalcCw1w2N", "StepCalcCw1", "StepCalcCw2", "StepCalcNpmiAndSum", "StepFilterSortNpmi"};
    public static final String[] Step_Output_Name = {"0.Cw1w2N", "1.Cw1", "2.Cw2", "3.NpmiAndSum", "4.FilteredSortedNpmi"};

    public static String minNpmi = "1"; 
    public static String relMinNpmi = "1";

    public static String getStepJarPath(int i){
        return "s3://" + PROJECT_NAME + "/" + Steps_Names[i] + ".jar";
    }
    public static String[] getStepArgs(int stepNum){
        String[] args;
        switch (stepNum){
            case 0:
                args = new String[]{
                    small_ENG_2Gram_path, 
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[0] 
                };
                break;
            case 1:
                args = new String[]{
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[0] + "/" , // input
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[1] // output
                };
                break;
            case 2:
                args = new String[]{
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[0] + "/", // input
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[2] // output
                };
                break;
            case 3:
                args = new String[]{
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[0] + "/", // input
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[1] + "/", // input
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[2] + "/", // input
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[3] // output
                };
                break;
            case 4:
                args = new String[]{
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[3] + "/", // input
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[4], // output
                    minNpmi,
                    relMinNpmi
                };
                break;
            default:
                args = new String[]{};
        }
        return args;
    }


    private static final String engStopWords = "a\nabout\nabove\nacross\nafter\nafterwards\nagain\nagainst\nall\nalmost\nalone\nalong\nalready\nalso\nalthough\nalways\nam\namong\namongst\namoungst\namount\nan\nand\nanother\nany\nanyhow\nanyone\nanything\nanyway\nanywhere\nare\naround\nas\nat\nback\nbe\nbecame\nbecause\nbecome\nbecomes\nbecoming\nbeen\nbefore\nbeforehand\nbehind\nbeing\nbelow\nbeside\nbesides\nbetween\nbeyond\nbill\nboth\nbottom\nbut\nby\ncall\ncan\ncannot\ncant\nco\ncomputer\ncon\ncould\ncouldnt\ncry\nde\ndescribe\ndetail\ndo\ndone\ndown\ndue\nduring\neach\neg\neight\neither\neleven\nelse\nelsewhere\nempty\nenough\netc\neven\never\nevery\neveryone\neverything\neverywhere\nexcept\nfew\nfifteen\nfify\nfill\nfind\nfire\nfirst\nfive\nfor\nformer\nformerly\nforty\nfound\nfour\nfrom\nfront\nfull\nfurther\nget\ngive\ngo\nhad\nhas\nhasnt\nhave\nhe\nhence\nher\nhere\nhereafter\nhereby\nherein\nhereupon\nhers\nherself\nhim\nhimself\nhis\nhow\nhowever\nhundred\ni\nie\nif\nin\ninc\nindeed\ninterest\ninto\nis\nit\nits\nitself\nkeep\nlast\nlatter\nlatterly\nleast\nless\nltd\nmade\nmany\nmay\nme\nmeanwhile\nmight\nmill\nmine\nmore\nmoreover\nmost\nmostly\nmove\nmuch\nmust\nmy\nmyself\nname\nnamely\nneither\nnever\nnevertheless\nnext\nnine\nno\nnobody\nnone\nnoone\nnor\nnot\nnothing\nnow\nnowhere\nof\noff\noften\non\nonce\none\nonly\nonto\nor\nother\nothers\notherwise\nour\nours\nourselves\nout\nover\nown\npart\nper\nperhaps\nplease\nput\nrather\nre\nsame\nsee\nseem\nseemed\nseeming\nseems\nserious\nseveral\nshe\nshould\nshow\nside\nsince\nsincere\nsix\nsixty\nso\nsome\nsomehow\nsomeone\nsomething\nsometime\nsometimes\nsomewhere\nstill\nsuch\nsystem\ntake\nten\nthan\nthat\nthe\ntheir\nthem\nthemselves\nthen\nthence\nthere\nthereafter\nthereby\ntherefore\ntherein\nthereupon\nthese\nthey\nthick\nthin\nthird\nthis\nthose\nthough\nthree\nthrough\nthroughout\nthru\nthus\nto\ntogether\ntoo\ntop\ntoward\ntowards\ntwelve\ntwenty\ntwo\nun\nunder\nuntil\nup\nupon\nus\nvery\nvia\nwas\nwe\nwell\nwere\nwhat\nwhatever\nwhen\nwhence\nwhenever\nwhere\nwhereafter\nwhereas\nwhereby\nwherein\nwhereupon\nwherever\nwhether\nwhich\nwhile\nwhither\nwho\nwhoever\nwhole\nwhom\nwhose\nwhy\nwill\nwith\nwithin\nwithout\nwould\nyet\nyou\nyour\nyours\nyourself\nyourselves";
    private static final String hebStopWords = "״\n׳\nשל\nרב\nפי\nעם\nעליו\nעליהם\nעל\nעד\nמן\nמכל\nמי\nמהם\nמה\nמ\nלמה\nלכל\nלי\nלו\nלהיות\nלה\nלא\nכן\nכמה\nכלי\nכל\nכי\nיש\nימים\nיותר\nיד\nי\nזה\nז\nועל\nומי\nולא\nוכן\nוכל\nוהיא\nוהוא\nואם\nו\nהרבה\nהנה\nהיו\nהיה\nהיא\nהזה\nהוא\nדבר\nד\nג\nבני\nבכל\nבו\nבה\nבא\nאת\nאשר\nאם\nאלה\nאל\nאך\nאיש\nאין\nאחת\nאחר\nאחד\nאז\nאותו\n?\n^\n?\n;\n:\n1\n.\n-\n*\n\"\n!\nשלשה\nבעל\nפני\n)\nגדול\nשם\nעלי\nעולם\nמקום\nלעולם\nלנו\nלהם\nישראל\nיודע\nזאת\nהשמים\nהזאת\nהדברים\nהדבר\nהבית\nהאמת\nדברי\nבמקום\nבהם\nאמרו\nאינם\nאחרי\nאותם\nאדם\n(\nחלק\nשני\nשכל\nשאר\nש\nר\nפעמים\nנעשה\nן\nממנו\nמלא\nמזה\nם\nלפי\nל\nכמו\nכבר\nכ\nזו\nומה\nולכל\nובין\nואין\nהן\nהיתה\nהא\nה\nבל\nבין\nבזה\nב\nאף\nאי\nאותה\nאו\nאבל\nא";
    private static final String ourStopWords = "[\n]\n-\n/\n<\n•\n■\n#\n$\n%\n—\n־\n£";
    private static final String stopWords = engStopWords + "\n" + hebStopWords + "\n" + ourStopWords;

    public static boolean isStopWord(String word) {
        // We add empty words to be a stopWord
        return word.length() < 2 || stopWords.contains(word) || doesContainNumbers(word);
    }

    public static boolean doesContainNumbers(String word) {
        return word.contains("0") | word.contains("1") | word.contains("2") | word.contains("3") | word.contains("4") | word.contains("5") | word.contains("6") | word.contains("7") | word.contains("8") | word.contains("9");
    }


}
