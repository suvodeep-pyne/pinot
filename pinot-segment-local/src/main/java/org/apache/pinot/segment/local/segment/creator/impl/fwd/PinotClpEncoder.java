package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import com.google.common.annotations.VisibleForTesting;
import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageDecoder;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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
  private final MessageDecoder _clpMessageDecoder;

  private final EncodedMessage _reusableEncodedMessage = new EncodedMessage();
  private final AtomicInteger _nPredefined = new AtomicInteger();
  private final AtomicInteger _nTotal = new AtomicInteger();

  public PinotClpEncoder(MessageEncoder clpMessageEncoder) {
    final List<String> predefinedTemplates = Arrays.asList(
        // Apache/Nginx access log template
        "\u0012 - - [\u0012] \"\u0012 \u0012 HTTP/\u0012\" \u0012 \u0012 \"\u0012\" \"\u0012\" \"-\"");
    _clpMessageEncoder = clpMessageEncoder;
    _clpMessageDecoder = new MessageDecoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    _templatePatterns = buildTemplatePatterns(predefinedTemplates);
  }

  private static Map<String, Pattern> buildTemplatePatterns(List<String> templates) {
    return templates.stream()
        .collect(
            Collectors.toMap(template -> template, template -> Pattern.compile(templateToRegex(template, PLACEHOLDERS)),
                (v1, v2) -> v1,  // Keep first value in case of duplicates
                LinkedHashMap::new  // Preserve order
            ));
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

  @VisibleForTesting
  boolean encodeMessageWithTemplate(String message, PinotClpEncodedMessage encodedMessage) {
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
        try {
          String decodedMessage =
              _clpMessageDecoder.decodeMessage(template, dictionaryVars, encodedMessage.getEncodedVars());
          if (!decodedMessage.equals(message)) {
            throw new IllegalStateException("Decoded message does not match original message");
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        _nPredefined.incrementAndGet();
        return true;
      }
    }
    return false;
  }

  public void encodeMessage(String message, PinotClpEncodedMessage encodedMessage)
      throws IOException {
    _nTotal.incrementAndGet();
    if (encodeMessageWithTemplate(message, encodedMessage)) {
      return;
    }

    _clpMessageEncoder.encodeMessage(message, _reusableEncodedMessage);
    encodedMessage.setLogType(_reusableEncodedMessage.getLogTypeAsString());
    encodedMessage.setDictionaryVars(_reusableEncodedMessage.getDictionaryVarsAsStrings());
    encodedMessage.setEncodedVars(_reusableEncodedMessage.getEncodedVars());
  }

  public AtomicInteger getnPredefined() {
    return _nPredefined;
  }
}
