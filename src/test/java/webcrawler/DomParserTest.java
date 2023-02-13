package webcrawler;

import webcrawler.crawler.DomParserBolt;
import stormlite.tuple.Fields;
import stormlite.tuple.Tuple;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class DomParserTest {
    String httpFile = "<!DOCTYPE html>\n" +
            "<html>\n" +
            "<head>\n" +
            "    <title>Register account</title>\n" +
            "</head>\n" +
            "<body>\n" +
            "<h1>Create Account for Milestone 2</h1>\n" +
            "<p>Please register a user name and password</p>\n" +
            "\n" +
            "<form method=\"POST\" action=\"/register\">\n" +
            "Name: <input type=\"text\" name=\"username\"/><br/>\n" +
            "Password: <input type=\"password\" name=\"password\"/><br/>\n" +
            "<input type=\"submit\" value=\"Create account\"/>\n" +
            "</form>\n" +
            "\n" +
            "</body>\n" +
            "</html>";
    String xmlFile = "<?xml version=\"1.0\" encoding=\"iso-8859-1\" ?>\n" +
            "   <rss version=\"2.0\">\n" +
            "   <channel>\n" +
            " \t  <title>NYT &gt; Africa</title>\n" +
            "\t\t<link>http://www.nytimes.com/pages/international/africa/index.html?partner=rssnyt</link>\n" +
            "\t\t<description>Find breaking news, world news, multimedia and opinion on Africa from South Africa, Egypt, Ethiopia, Libya, Rwanda, Kenya, Morocco, Zimbabwe, Sudan and Algeria. </description>\n" +
            "\t\t<copyright>Copyright 2006 The New York Times Company</copyright>\n" +
            "\t\t<language>en-us</language>\n" +
            "\t\t<lastBuildDate>Thu,  2 Feb 2006 23:05:01 EST</lastBuildDate>\n" +
            "\t\t<image>\n" +
            "\t\t\t<url>http://graphics.nytimes.com/images/section/NytSectionHeader.gif</url>\n" +
            "\t\t\t<title>NYT &gt; Africa</title>\n" +
            "\t\t\t<link>http://www.nytimes.com/pages/international/africa/index.html</link>\n" +
            "\t\t</image>\n" +
            "\t\t<item>\n" +
            "\t\t\t<title>Egypt Insists That Hamas Stop Violence</title>\n" +
            "\t\t\t<link>http://www.nytimes.com/2006/02/02/international/middleeast/02egypt.html?ex=1296536400&#38;en=06debaf57bef24dc&#38;ei=5088&#38;partner=rssnyt&#38;emc=rss</link>\n" +
            "\t\t\t<description>Egypt insisted that Hamas confirm existing agreements between Israel and the Palestinians and recognize Israel&#39;s legitimacy.</description>\n" +
            "\t\t\t<author>STEVEN ERLANGER</author>\n" +
            "\t\t\t<pubDate>Thu, 02 Feb 2006 00:00:00 EDT</pubDate>\n" +
            "\t\t\t<guid isPermaLink=\"false\">http://www.nytimes.com/2006/02/02/international/middleeast/02egypt.html</guid>\n" +
            "\t\t</item>\n" +
            "\t\t<item>\n" +
            "\t\t\t<title>World Briefings: Africa, Asia, Middle East, United Nations, Europe</title>\n" +
            "\t\t\t<link>http://www.nytimes.com/2006/02/03/international/03briefs.html?ex=1296622800&#38;en=17c287b37f35e7d9&#38;ei=5088&#38;partner=rssnyt&#38;emc=rss</link>\n" +
            "\t\t\t<description>AFRICA.</description>\n" +
            "\t\t\t<author>(AP)</author>\n" +
            "\t\t\t<pubDate>Fri, 03 Feb 2006 00:00:00 EDT</pubDate>\n" +
            "\t\t\t<guid isPermaLink=\"false\">http://www.nytimes.com/2006/02/03/international/03briefs.html</guid>\n" +
            "\t\t</item>\n" +
            "\t\t<item>\n" +
            "\t\t\t<title>World Briefing: Africa, Americas, Europe and Asia</title>\n" +
            "\t\t\t<link>http://www.nytimes.com/2006/02/02/international/02briefs.html?ex=1296536400&#38;en=cd101688c565f27f&#38;ei=5088&#38;partner=rssnyt&#38;emc=rss</link>\n" +
            "\t\t\t<description>AFRICA.</description>\n" +
            "\t\t\t<pubDate>Thu, 02 Feb 2006 00:00:00 EDT</pubDate>\n" +
            "\t\t\t<guid isPermaLink=\"false\">http://www.nytimes.com/2006/02/02/international/02briefs.html</guid>\n" +
            "\t\t</item>\n" +
            "\t\t<item>\n" +
            "\t\t\t<title>Loan for Foreign Mining in Ghana Approved</title>\n" +
            "\t\t\t<link>http://www.nytimes.com/2006/02/01/international/africa/01africa.html?ex=1296450000&#38;en=6ef2e47c65509682&#38;ei=5088&#38;partner=rssnyt&#38;emc=rss</link>\n" +
            "\t\t\t<description>The World Bank&#39;s investment agency said the loan was approved on the condition that Newmont Mining meet stringent social and environmental standards.</description>\n" +
            "\t\t\t<author>CELIA W. DUGGER</author>\n" +
            "\t\t\t<pubDate>Wed, 01 Feb 2006 00:00:00 EDT</pubDate>\n" +
            "\t\t\t<guid isPermaLink=\"false\">http://www.nytimes.com/2006/02/01/international/africa/01africa.html</guid>\n" +
            "\t\t</item>\n" +
            "\t\t<item>\n" +
            "\t\t\t<title>World Briefing: Asia, Middle East, Americas, Europe and Africa</title>\n" +
            "\t\t\t<link>http://www.nytimes.com/2006/02/01/international/01briefs.html?ex=1296450000&#38;en=a744dabed3339202&#38;ei=5088&#38;partner=rssnyt&#38;emc=rss</link>\n" +
            "\t\t\t<description>ASIA.</description>\n" +
            "\t\t\t<author>SALMAN MASOOD (NYT)</author>\n" +
            "\t\t\t<pubDate>Wed, 01 Feb 2006 00:00:00 EDT</pubDate>\n" +
            "\t\t\t<guid isPermaLink=\"false\">http://www.nytimes.com/2006/02/01/international/01briefs.html</guid>\n" +
            "\t\t</item>\n" +
            "\t\t<item>\n" +
            "\t\t\t<title>Khartoum Journal: Sudan Leader Waits, and Waits, for His Ship to Come In</title>\n" +
            "\t\t\t<link>http://www.nytimes.com/2006/01/31/international/africa/31khartoum.html?ex=1296363600&#38;en=c514139910f2888c&#38;ei=5088&#38;partner=rssnyt&#38;emc=rss</link>\n" +
            "\t\t\t<description>For a war-torn, impoverished country, a gigantic, luxurious presidential yacht.</description>\n" +
            "\t\t\t<author>MARC LACEY</author>\n" +
            "\t\t\t<pubDate>Tue, 31 Jan 2006 00:00:00 EDT</pubDate>\n" +
            "\t\t\t<guid isPermaLink=\"false\">http://www.nytimes.com/2006/01/31/international/africa/31khartoum.html</guid>\n" +
            "\t\t</item>\n" +
            "\t</channel>\n" +
            "</rss>\n";
    boolean done = false;
    Fields schema = new Fields("url", "file", "done");

    @Test
    public void HttpParserTest() {
        String url = "http://test.html";
        List<Object> objects = new LinkedList<>();
        objects.add(url);
        objects.add(httpFile);
        objects.add(done);
        Tuple input = new Tuple(schema, objects);

        DomParserBolt parser = new DomParserBolt();
        parser.execute(input);
        assert parser.getEventList().size() == 43;
    }

    @Test
    public void XmlParserTest() {
        String url = "http://test.xml";
        List<Object> objects = new LinkedList<>();
        objects.add(url);
        objects.add(xmlFile);
        objects.add(done);
        Tuple input = new Tuple(schema, objects);

        DomParserBolt parser = new DomParserBolt();
        parser.execute(input);
        assert parser.getEventList().size() == 217;
    }

    @Test
    public void DoneParserTest() {
        String url = "http://test.xml";
        List<Object> objects = new LinkedList<>();
        objects.add(url);
        objects.add(xmlFile);
        objects.add(true);
        Tuple input = new Tuple(schema, objects);

        DomParserBolt parser = new DomParserBolt();
        parser.execute(input);

        assert parser.getEventList().size() == 0;
    }
}
