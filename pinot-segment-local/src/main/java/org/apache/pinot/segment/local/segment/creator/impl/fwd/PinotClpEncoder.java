package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;


@NotThreadSafe
public class PinotClpEncoder {
  private static final char PLACEHOLDER_LONG = '\u0011'; // DC1
  private static final char PLACEHOLDER_STRING = '\u0012'; // DC2
  private static final char PLACEHOLDER_FLOAT = '\u0013'; // DC3
  private static final char[] PLACEHOLDERS = {PLACEHOLDER_LONG, PLACEHOLDER_STRING, PLACEHOLDER_FLOAT};

  private final Map<String, Pattern> _templatePatterns;
  private final MessageEncoder _clpMessageEncoder;

  private final EncodedMessage _reusableEncodedMessage = new EncodedMessage();

  public PinotClpEncoder(MessageEncoder clpMessageEncoder) {
    final List<String> predefinedTemplates = Arrays.asList(
        // Apache/Nginx access log template
        "\u0012 - - [\u0012] \"\u0012 \u0012 HTTP/\u0012\" \u0012 \u0012 \"\u0012\" \"\u0012\" \"-\""
    );
    _clpMessageEncoder = clpMessageEncoder;
    _templatePatterns = buildTemplatePatterns(predefinedTemplates);
  }

  private static Map<String, Pattern> buildTemplatePatterns(List<String> templates) {
    return templates.stream()
        .collect(Collectors.toMap(
            template -> template,
            template -> Pattern.compile(templateToRegex(template, PLACEHOLDERS)),
            (v1, v2) -> v1,  // Keep first value in case of duplicates
            LinkedHashMap::new  // Preserve order
        ));
  }

  public boolean encodeMessageWithTemplate(String message, PinotClpEncodedMessage encodedMessage) {
    for (Map.Entry<String, Pattern> entry : _templatePatterns.entrySet()) {
      Matcher matcher = entry.getValue().matcher(message);
      if (matcher.matches()) {
        String template = entry.getKey();
        encodedMessage.setLogType(template);
        
        String[] dictionaryVars = new String[matcher.groupCount()];
        for (int j = 1; j <= matcher.groupCount(); j++) {
          dictionaryVars[j - 1] = matcher.group(j);
        }
        encodedMessage.setDictionaryVars(dictionaryVars);
        
        return true;
      }
    }
    return false;
  }

  // Existing helper methods remain unchanged
  private static String templateToRegex(String template, char[] placeholders) {
    String[] parts = template.split(placeholdersRegex(placeholders));

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

  public void encodeMessage(String message, PinotClpEncodedMessage encodedMessage) throws IOException {
    if (encodeMessageWithTemplate(message, encodedMessage)) {
      return;
    }

    _clpMessageEncoder.encodeMessage(message, _reusableEncodedMessage);
    encodedMessage.setLogType(_reusableEncodedMessage.getLogTypeAsString());
    encodedMessage.setDictionaryVars(_reusableEncodedMessage.getDictionaryVarsAsStrings());
    encodedMessage.setEncodedVars(_reusableEncodedMessage.getEncodedVars());
  }
}
