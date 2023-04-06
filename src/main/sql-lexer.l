%{
/*
   File:  sql-lexer.l

   Description:
      This is the "lex" input file for the Aerospike SQL lexical analyzer.

         * The single public API function is:  [Declared in "sql-lexer.h".]

             int as_sql_lexer(char *input, char **token)

         * Make the initial call with a SQL string as the first argument to fetch the first token.
             (Note that this can be done again at any time to begin scanning a new SQL string.)

         * Call again with NULL as the first argument until the return value is non-zero to fetch successive tokens.

         * If the return value is 0, the token returned in the second argument is valid until the next API call.
             (Note that the caller is responsible for copying the token value (if desired) before calling the API again.)

         * A return value of -1 means the end of tokens in the initial SQL string has been reached.

         * A return value of -2 means lexer error, such as an unknown character or an unterminated string literal, has occurred.

         * This lexer recognizes the following types of tokens:
             - Strings (single and double quote delimited)
             - Standard SQL key words
             - Identifiers
             - Numbers (signed or unsigned fixed or floating point without exponent)
             - Relational operators
             - Punctuation characters

         * All recognized tokens are returned as strings.

         * Tokens that are string literals include matching leading and trailing quote characters.

   Copyright (c) 2012, Aerospike.  All rights reserved.
*/

#include <ctype.h>
#include <stdbool.h>
#include "sql-lexer.h"

/* Type for lexer function return values. */
enum {
   OK = 0,       // Success:  Token is valid.  Call iterator again with NULL as the first argument for the next token.
   DONE = -1,    // End of token stream reached:  Token is NULL, and iteration is terminated.
   ERROR = -2    // Lexer error:  Token is NULL, and iteration is terminated.
};

/* Maximum length of a reserved word. */
#define MAX_TOKEN_LEN   100

/* Return the given token as a string. */
#define RETURN(token)  { gToken = token; return 1; }

/* Return the given token as an uppercase string. */
#define RETURN_UC(token)  { gToken = uc(token); return 1; }

/* Signal a lexer error. */
#define ERROR() { gError = true; BEGIN 0; return 0; }

/* Latest token returned by the lexer. */
static char *gToken = NULL;

/* Has a lexer error been encountered? */
static bool gError = false;

/* Function prototypes. */
//static char *uc(char *str);

%}

%START SQUOTE DQUOTE

%option noyywrap

%%

 /* Strings */

<SQUOTE>\'   { BEGIN 0; RETURN(yytext); }
<SQUOTE><<EOF>>   { ERROR(); }
<SQUOTE>[^\']*   { yymore(); }
<DQUOTE>\'   { yymore(); }
\'   { BEGIN SQUOTE; yymore(); }

<DQUOTE>\"   { BEGIN 0; RETURN(yytext); }
<DQUOTE><<EOF>>   { ERROR(); }
<DQUOTE>[^\"]*   { yymore(); }
\"   { BEGIN DQUOTE; yymore(); }

 /* Identifiers */

[A-Za-z][A-Za-z0-9_:-]*   { RETURN(yytext); }

[0-9][0-9A-Za-z_:-]*	{ RETURN(yytext); } 

 /* Numbers */

[+-]?[0-9]+(\.[0-9]*)?   { RETURN(yytext); }

[+-]?[0-9]*\.[0-9]+   { RETURN(yytext); }

 /* Hex Numbers */

[+-]?0x[a-fA-F0-9]+   { RETURN(yytext); }


 /* Operators */

"="   { RETURN(yytext); }
"<>"   { RETURN(yytext); }
"<"   { RETURN(yytext); }
"<="   { RETURN(yytext); }
">"   { RETURN(yytext); }
">="   { RETURN(yytext); }

 /* Punctuation */

"("   { RETURN(yytext); }
")"   { RETURN(yytext); }
","   { RETURN(yytext); }
";"   { RETURN(yytext); }
"."   { RETURN(yytext); }
"*"   { RETURN(yytext); }
"/"   { RETURN(yytext); }
"["   { RETURN(yytext); }
"]"   { RETURN(yytext); }

 /* Whitespace */

[ \t\n\r]+   /* Eat it. */ ;

 /* Anything else is an error. */

.   { ERROR(); }

%%

/*
 * uc():
 *  Return an uppercase version of the supplied string argument.
 *  [Note:  Not reentrant.]
 */
/*
static char *uc(char *str)
{
    static char str_uc[MAX_TOKEN_LEN];
    char *str_uc_p = str_uc;

    while (*str) {
       *str_uc_p++ = toupper(*str);
       str++;
    }
    *str_uc_p = '\0';

    return str_uc;
}
*/

/*
 * as_sql_lexer():
 *   Tokenize the given input string according to the lexical structure of SQL.
 *   This function is an iterator passing back each token in sequence as a string via the second argument.
 *   To initialize, call this function with the first argument being the SQL string and the second
 *   argument being a pointer to the first token.  If the return value is 0, you may call the function
 *   again with NULL for the first argument to receive a pointer to the next token.  Repeat until a non-0
 *   value is returned.  Depending upon the return value (see below), the token will either be valid or NULL.
 *   Iteration of a new SQL string may be begun at any time.
 *
 *   Caveats:  This function is not reentrant, as it stores a single lexer state internally.
 *
 *   Return values:
 *
 *     0 -- Success:  Token is valid.  Call again with NULL as the first argument to receive the next token.
 *    -1 -- End of token stream reached:  Token is NULL, and iteration is terminated.
 *    -2 -- Lexer error:  Token is NULL, and iteration is terminated.
 */
int as_sql_lexer(char *input, char **token, bool peek)
{
   static char *sql_str;
   static bool done = false;

   int token_id;
   int rv = OK;

   if (input) {
      /*
       * Enlarge the input buffer by 1. This enables the use of peek on the first token.
       * For whatever reason, using unput on the first token causes a buffer underflow.
       */
      char* tmp_input = malloc(sizeof(char) * (strlen(input) + 2));
      memcpy(tmp_input + 1, input, strlen(input) + 1);
      tmp_input[0] = ' ';

      gError = false;
      done = false;
      sql_str = tmp_input;
      yy_scan_string(sql_str);
      free(tmp_input);
   }

   if (done) {
      if (token) {
        *token = '\0';
      }
      return ERROR;
   }

   if ((token_id = yylex())) {
      if (token) {
          *token = gToken;
      }

      if (peek) {
         // Allows for peeking at the next token without consuming it.
         char *yycopy = strdup( yytext );
         for (int i = yyleng - 1; i >= 0; --i) {
            unput(yycopy[i]);
         }
         *token = yycopy;
      }
      rv = OK;
   } else if (gError) {
      if (token) {
         *token = '\0';
      }
      done = true;
      rv = ERROR;
   } else {
      if (token) {
         *token = '\0';
      }
      done = true;
      rv = DONE;
   }

   return rv;
}

