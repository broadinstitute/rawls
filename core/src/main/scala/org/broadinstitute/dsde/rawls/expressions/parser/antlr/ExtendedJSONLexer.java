// Generated from ExtendedJSON.g4 by ANTLR 4.8
package org.broadinstitute.dsde.rawls.expressions.parser.antlr;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class ExtendedJSONLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.8", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, STRING=11, NUMBER=12, SYMBOL=13, WS=14;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
			"T__9", "STRING", "ESC", "UNICODE", "HEX", "SAFECODEPOINT", "NUMBER", 
			"INT", "EXP", "SYMBOL", "WS"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'{'", "','", "'}'", "':'", "'['", "']'", "'.'", "'true'", "'false'", 
			"'null'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, null, null, null, null, null, null, null, null, null, null, "STRING", 
			"NUMBER", "SYMBOL", "WS"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public ExtendedJSONLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "ExtendedJSON.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\20\u008d\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3"+
		"\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\n\3\n\3\13"+
		"\3\13\3\13\3\13\3\13\3\f\3\f\3\f\7\fM\n\f\f\f\16\fP\13\f\3\f\3\f\3\r\3"+
		"\r\3\r\5\rW\n\r\3\16\3\16\3\16\3\16\3\16\3\16\3\17\3\17\3\20\3\20\3\21"+
		"\5\21d\n\21\3\21\3\21\3\21\6\21i\n\21\r\21\16\21j\5\21m\n\21\3\21\5\21"+
		"p\n\21\3\22\3\22\3\22\7\22u\n\22\f\22\16\22x\13\22\5\22z\n\22\3\23\3\23"+
		"\5\23~\n\23\3\23\3\23\3\24\6\24\u0083\n\24\r\24\16\24\u0084\3\25\6\25"+
		"\u0088\n\25\r\25\16\25\u0089\3\25\3\25\2\2\26\3\3\5\4\7\5\t\6\13\7\r\b"+
		"\17\t\21\n\23\13\25\f\27\r\31\2\33\2\35\2\37\2!\16#\2%\2\'\17)\20\3\2"+
		"\13\n\2$$\61\61^^ddhhppttvv\5\2\62;CHch\5\2\2!$$^^\3\2\62;\3\2\63;\4\2"+
		"GGgg\4\2--//\5\2C\\aac|\5\2\13\f\17\17\"\"\2\u0092\2\3\3\2\2\2\2\5\3\2"+
		"\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21"+
		"\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2!\3\2\2\2\2\'\3\2\2\2"+
		"\2)\3\2\2\2\3+\3\2\2\2\5-\3\2\2\2\7/\3\2\2\2\t\61\3\2\2\2\13\63\3\2\2"+
		"\2\r\65\3\2\2\2\17\67\3\2\2\2\219\3\2\2\2\23>\3\2\2\2\25D\3\2\2\2\27I"+
		"\3\2\2\2\31S\3\2\2\2\33X\3\2\2\2\35^\3\2\2\2\37`\3\2\2\2!c\3\2\2\2#y\3"+
		"\2\2\2%{\3\2\2\2\'\u0082\3\2\2\2)\u0087\3\2\2\2+,\7}\2\2,\4\3\2\2\2-."+
		"\7.\2\2.\6\3\2\2\2/\60\7\177\2\2\60\b\3\2\2\2\61\62\7<\2\2\62\n\3\2\2"+
		"\2\63\64\7]\2\2\64\f\3\2\2\2\65\66\7_\2\2\66\16\3\2\2\2\678\7\60\2\28"+
		"\20\3\2\2\29:\7v\2\2:;\7t\2\2;<\7w\2\2<=\7g\2\2=\22\3\2\2\2>?\7h\2\2?"+
		"@\7c\2\2@A\7n\2\2AB\7u\2\2BC\7g\2\2C\24\3\2\2\2DE\7p\2\2EF\7w\2\2FG\7"+
		"n\2\2GH\7n\2\2H\26\3\2\2\2IN\7$\2\2JM\5\31\r\2KM\5\37\20\2LJ\3\2\2\2L"+
		"K\3\2\2\2MP\3\2\2\2NL\3\2\2\2NO\3\2\2\2OQ\3\2\2\2PN\3\2\2\2QR\7$\2\2R"+
		"\30\3\2\2\2SV\7^\2\2TW\t\2\2\2UW\5\33\16\2VT\3\2\2\2VU\3\2\2\2W\32\3\2"+
		"\2\2XY\7w\2\2YZ\5\35\17\2Z[\5\35\17\2[\\\5\35\17\2\\]\5\35\17\2]\34\3"+
		"\2\2\2^_\t\3\2\2_\36\3\2\2\2`a\n\4\2\2a \3\2\2\2bd\7/\2\2cb\3\2\2\2cd"+
		"\3\2\2\2de\3\2\2\2el\5#\22\2fh\7\60\2\2gi\t\5\2\2hg\3\2\2\2ij\3\2\2\2"+
		"jh\3\2\2\2jk\3\2\2\2km\3\2\2\2lf\3\2\2\2lm\3\2\2\2mo\3\2\2\2np\5%\23\2"+
		"on\3\2\2\2op\3\2\2\2p\"\3\2\2\2qz\7\62\2\2rv\t\6\2\2su\t\5\2\2ts\3\2\2"+
		"\2ux\3\2\2\2vt\3\2\2\2vw\3\2\2\2wz\3\2\2\2xv\3\2\2\2yq\3\2\2\2yr\3\2\2"+
		"\2z$\3\2\2\2{}\t\7\2\2|~\t\b\2\2}|\3\2\2\2}~\3\2\2\2~\177\3\2\2\2\177"+
		"\u0080\5#\22\2\u0080&\3\2\2\2\u0081\u0083\t\t\2\2\u0082\u0081\3\2\2\2"+
		"\u0083\u0084\3\2\2\2\u0084\u0082\3\2\2\2\u0084\u0085\3\2\2\2\u0085(\3"+
		"\2\2\2\u0086\u0088\t\n\2\2\u0087\u0086\3\2\2\2\u0088\u0089\3\2\2\2\u0089"+
		"\u0087\3\2\2\2\u0089\u008a\3\2\2\2\u008a\u008b\3\2\2\2\u008b\u008c\b\25"+
		"\2\2\u008c*\3\2\2\2\17\2LNVcjlovy}\u0084\u0089\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}