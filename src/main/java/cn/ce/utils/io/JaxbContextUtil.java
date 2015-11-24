package cn.ce.utils.io;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.UUID;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.xml.sax.SAXException;

public class JaxbContextUtil {

	public static void marshall(Object toSerial, String targetFilePath)
			throws IOException {
		File file = new File(targetFilePath);
		FileUtils.write(file, "", "utf-8", false);
		FileOutputStream fo = new FileOutputStream(file);
		JaxbContextUtil.marshall(toSerial, fo, null);
	}

	public static void marshall(Object toSerial, OutputStream out,
			String xsdName) throws IOException {
		try {
			JAXBContext ctx = JAXBContext.newInstance(toSerial.getClass());
			Marshaller m = ctx.createMarshaller();
			m.setProperty("jaxb.formatted.output", true);
			if (!StringUtils.isBlank(xsdName)) {
				m.setProperty("jaxb.noNamespaceSchemaLocation", xsdName);
			}
			m.marshal(toSerial, out);
		} catch (JAXBException je) {
			je.printStackTrace();
			throw new IOException(je.getMessage());
		} finally {
			out.close();
		}
	}

	public static String marshallToString(Object toSerial, String xsdName,
			String encoding) throws IOException {
		// ------------------------------------生成xml字符串

		File tempFile = new File("engin_marshallToString.tmp"
				+ System.currentTimeMillis() + UUID.randomUUID());
		FileOutputStream out = new FileOutputStream(tempFile);
		JaxbContextUtil.marshall(toSerial, out, xsdName);
		String xmlInfo = FileUtils.readFileToString(tempFile, encoding);
		tempFile.delete();
		return xmlInfo;
	}

	public static <T> T unmarshal(Class<T> docClass, InputStream inputStream,
			URL xsdURL) throws JAXBException, IOException, SAXException {
		try {
			JAXBContext ctx = JAXBContext.newInstance(docClass);
			Unmarshaller u = ctx.createUnmarshaller();
			if (xsdURL != null) {
				SchemaFactory schemaFactory = SchemaFactory
						.newInstance("http://www.w3.org/2001/XMLSchema");
				Schema schema = schemaFactory.newSchema(xsdURL);
				u.setSchema(schema);
			}
			T t = (T) u.unmarshal(inputStream);
			return t;
		} catch (JAXBException je) {
			je.printStackTrace();
			throw new IOException(je.getMessage());
		} finally {
			inputStream.close();
		}

	}

	public static <T> T unmarshallToObject(Class<T> docClass, String infoXml,
			URL xsdURL, String encoding) throws IOException, JAXBException,
			SAXException {
		ByteArrayInputStream in = new ByteArrayInputStream(
				infoXml.getBytes(encoding));
		T t = (T) JaxbContextUtil.unmarshal(docClass, in, xsdURL);
		return t;
	}
}
