package com.jumpstart.utils;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class InMemoryOutputFile implements OutputFile {

	private final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

	@Override
	public PositionOutputStream create(long blockSizeHint) throws IOException {
		return new PositionOutputStream() {
			private long position = 0;

			@Override
			public void write(int b) throws IOException {
				outputStream.write(b);
				position++;
			}

			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				outputStream.write(b, off, len);
				position += len;
			}

			@Override
			public long getPos() throws IOException {
				return position;
			}

			@Override
			public void close() throws IOException {
				outputStream.close();
			}
		};
	}

	@Override
	public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
		return create(blockSizeHint);
	}

	@Override
	public boolean supportsBlockSize() {
		return false;
	}

	@Override
	public long defaultBlockSize() {
		return 0;
	}

	public byte[] getContent() {
		return outputStream.toByteArray();
	}
}
