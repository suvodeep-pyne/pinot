package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class PinotClpEncoder {
  public static final String LOG_TEMPLATE =
      "\u0012 - - [\u0012] \"\u0012 \u0012 HTTP/\u0012\" \u0012 \u0012 \"\u0012\" \"\u0012\" \"-\"";
  private static final char VAR_LONG = '\u0011'; // DC1
  private static final char VAR_STRING = '\u0012'; // DC2
  private static final char VAR_FLOAT = '\u0013'; // DC3
  private static final char[] PLACEHOLDERS = {VAR_LONG, VAR_STRING, VAR_FLOAT};
  private static final String LOG_TEMPLATE_REGEX = templateToRegex(LOG_TEMPLATE, PLACEHOLDERS);

  private final MessageEncoder _clpMessageEncoder;

  public PinotClpEncoder(MessageEncoder clpMessageEncoder) {
    _clpMessageEncoder = clpMessageEncoder;
  }

  public static String templateToRegex(String template, char[] placeholders) {
    String[] parts = template.split(placeholdersRegex(placeholders));

    // If no parts exist at all, handle that edge case
    if (parts.length == 0) {
      return Pattern.quote(template);
    }

    StringBuilder sb = new StringBuilder();
    sb.append("^");

    for (int i = 0; i < parts.length - 1; i++) {
      sb.append(Pattern.quote(parts[i]));
      sb.append("(.+?)");
    }
    sb.append(Pattern.quote(parts[parts.length - 1]));
    sb.append("$");
    return sb.toString();
  }

  private static String placeholdersRegex(char[] placeholders) {
    StringBuilder placeholderRegex = new StringBuilder("[");
    for (char ch : placeholders) {
      placeholderRegex.append("\\u");
      placeholderRegex.append(String.format("%04x", (int) ch));
    }
    placeholderRegex.append("]+");
    return placeholderRegex.toString();
  }

  private static boolean isPlaceholder(char c, char[] placeholders) {
    for (char placeholder : placeholders) {
      if (c == placeholder) {
        return true;
      }
    }
    return false;
  }

  public void encodeMessage(String message, EncodedMessage encodedMessage)
      throws IOException {
    _clpMessageEncoder.encodeMessage(message, encodedMessage);
  }

  public boolean encodeMessageWithTemplate(String message, PinotClpEncodedMessage encodedMessage) {
    Pattern pattern = Pattern.compile(LOG_TEMPLATE_REGEX);
    Matcher matcher = pattern.matcher(message);

    if (!matcher.matches()) {
      return false;
    }

    encodedMessage.setLogType(LOG_TEMPLATE);
    String[] dictionaryVars = new String[matcher.groupCount()];
    for (int i = 1; i <= matcher.groupCount(); i++) {
      dictionaryVars[i - 1] = matcher.group(i);
    }
    encodedMessage.setDictionaryVars(dictionaryVars);

    return true;
  }
}
