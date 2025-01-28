package org.apache.pinot.segment.local.segment.creator.impl.fwd;

import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.MessageDecoder;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PinotClpEncoderTest {
  private static final String LOG_LINE_1 =
      "31.56.96.51 - - [22/Jan/2019:03:56:16 +0330] \"GET /image/60844/productModel/200x200 HTTP/1.1\" 200 5667 \"https://www.zanbil.ir/m/filter/b113\" \"Mozilla/5.0 (Linux; Android 6.0; ALE-L21 Build/HuaweiALE-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.158 Mobile Safari/537.36\" \"-\"";
  private PinotClpEncoder _clpEncoder;
  private MessageDecoder _clpDecoder;

  @BeforeMethod
  public void setUp() {
    _clpEncoder = new PinotClpEncoder(
        new MessageEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
            BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1));
    _clpDecoder = new MessageDecoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
  }

  @Test
  public void testEncodeDecode1()
      throws IOException {
    PinotClpEncodedMessage encodedMessage = new PinotClpEncodedMessage();
    _clpEncoder.encodeMessage(LOG_LINE_1, encodedMessage);
    String decodedMessage =
        _clpDecoder.decodeMessage(encodedMessage.getLogType(), encodedMessage.getDictionaryVars(),
            encodedMessage.getEncodedVars());

     Assert.assertEquals(decodedMessage, LOG_LINE_1);
  }

  @Test
  public void testEncodeDecode2()
      throws IOException {
    PinotClpEncodedMessage encodedMessage = new PinotClpEncodedMessage();
    _clpEncoder.encodeMessageWithTemplate(LOG_LINE_1, encodedMessage);
    String decodedMessage =
        _clpDecoder.decodeMessage(encodedMessage.getLogType(), encodedMessage.getDictionaryVars(),
            encodedMessage.getEncodedVars());

    Assert.assertEquals(decodedMessage, LOG_LINE_1);
  }
}