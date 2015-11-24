package cn.ce.dvs.manager;

import java.io.Closeable;

public interface HAManager extends Closeable {
	public void start();
	public void close();
}
