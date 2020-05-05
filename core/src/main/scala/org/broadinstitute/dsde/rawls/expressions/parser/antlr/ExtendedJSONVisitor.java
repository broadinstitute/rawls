// Generated from ExtendedJSON.g4 by ANTLR 4.8
package org.broadinstitute.dsde.rawls.expressions.parser.antlr;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link ExtendedJSONParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface ExtendedJSONVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link ExtendedJSONParser#obj}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitObj(ExtendedJSONParser.ObjContext ctx);
	/**
	 * Visit a parse tree produced by {@link ExtendedJSONParser#pair}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPair(ExtendedJSONParser.PairContext ctx);
	/**
	 * Visit a parse tree produced by {@link ExtendedJSONParser#arr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitArr(ExtendedJSONParser.ArrContext ctx);
	/**
	 * Visit a parse tree produced by {@link ExtendedJSONParser#lookup}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLookup(ExtendedJSONParser.LookupContext ctx);
	/**
	 * Visit a parse tree produced by {@link ExtendedJSONParser#value}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitValue(ExtendedJSONParser.ValueContext ctx);
	/**
	 * Visit a parse tree produced by {@link ExtendedJSONParser#literal}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitLiteral(ExtendedJSONParser.LiteralContext ctx);
}