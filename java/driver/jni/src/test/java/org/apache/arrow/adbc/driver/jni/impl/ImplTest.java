/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.adbc.driver.jni.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

public class ImplTest {
  @Test
  void emptyHeap() throws Exception {
    ByteBuffer buf = ByteBuffer.allocate(0);
    assertThat(JniLoader.INSTANCE.internalGetByteBuffer(buf)).isEmpty();

    buf = ByteBuffer.allocate(16);
    buf.position(16);
    assertThat(buf.remaining()).isEqualTo(0);
    assertThat(JniLoader.INSTANCE.internalGetByteBuffer(buf)).isEmpty();
  }

  @Test
  void emptyDirect() throws Exception {
    ByteBuffer buf = ByteBuffer.allocateDirect(0);
    assertThat(buf.isDirect()).isTrue();
    assertThat(JniLoader.INSTANCE.internalGetByteBuffer(buf)).isEmpty();

    buf = ByteBuffer.allocateDirect(16);
    assertThat(buf.isDirect()).isTrue();
    buf.position(16);
    assertThat(buf.remaining()).isEqualTo(0);
    assertThat(JniLoader.INSTANCE.internalGetByteBuffer(buf)).isEmpty();
  }

  @Test
  void emptyArray() throws Exception {
    ByteBuffer buf = ByteBuffer.wrap(new byte[0]);
    assertThat(buf.hasArray()).isTrue();
    assertThat(JniLoader.INSTANCE.internalGetByteBuffer(buf)).isEmpty();

    buf = ByteBuffer.wrap(new byte[16]);
    buf.position(16);
    assertThat(buf.hasArray()).isTrue();
    assertThat(buf.remaining()).isEqualTo(0);
    assertThat(JniLoader.INSTANCE.internalGetByteBuffer(buf)).isEmpty();
  }

  @Test
  void emptySlow() throws Exception {
    ByteBuffer buf = ByteBuffer.wrap(new byte[0]).asReadOnlyBuffer();
    // take the slow path
    assertThat(buf.hasArray()).isFalse();
    assertThat(buf.isDirect()).isFalse();
    assertThat(JniLoader.INSTANCE.internalGetByteBuffer(buf)).isEmpty();

    buf = ByteBuffer.wrap(new byte[16]);
    buf.position(16);
    buf = buf.asReadOnlyBuffer();
    assertThat(buf.hasArray()).isFalse();
    assertThat(buf.isDirect()).isFalse();
    assertThat(buf.remaining()).isEqualTo(0);
    assertThat(JniLoader.INSTANCE.internalGetByteBuffer(buf)).isEmpty();
  }

  @Test
  void offsetHeap() throws Exception {
    ByteBuffer buf = ByteBuffer.allocate(4);
    buf.put((byte) 0);
    buf.put((byte) 1);
    buf.put((byte) 2);
    buf.put((byte) 3);
    buf.position(1);
    assertThat(buf.remaining()).isEqualTo(3);
    assertThat(JniLoader.INSTANCE.internalGetByteBuffer(buf)).isEqualTo(new byte[] {1, 2, 3});
  }

  @Test
  void offsetDirect() throws Exception {
    ByteBuffer buf = ByteBuffer.allocateDirect(4);
    buf.put((byte) 0);
    buf.put((byte) 1);
    buf.put((byte) 2);
    buf.put((byte) 3);
    buf.position(1);
    assertThat(buf.remaining()).isEqualTo(3);
    assertThat(JniLoader.INSTANCE.internalGetByteBuffer(buf)).isEqualTo(new byte[] {1, 2, 3});
  }

  @Test
  void offsetArray() throws Exception {
    ByteBuffer buf = ByteBuffer.wrap(new byte[] {(byte) 0, (byte) 1, (byte) 2, (byte) 3});
    buf.position(1);
    assertThat(buf.hasArray()).isTrue();
    assertThat(buf.remaining()).isEqualTo(3);
    assertThat(JniLoader.INSTANCE.internalGetByteBuffer(buf)).isEqualTo(new byte[] {1, 2, 3});
  }

  @Test
  void offsetSlow() throws Exception {
    ByteBuffer buf = ByteBuffer.wrap(new byte[] {(byte) 0, (byte) 1, (byte) 2, (byte) 3});
    buf.position(1);
    buf = buf.asReadOnlyBuffer();
    assertThat(buf.hasArray()).isFalse();
    assertThat(buf.isDirect()).isFalse();
    assertThat(buf.remaining()).isEqualTo(3);
    assertThat(JniLoader.INSTANCE.internalGetByteBuffer(buf)).isEqualTo(new byte[] {1, 2, 3});
  }

  @Test
  void childReferencesCloses() throws Exception {
    ChildReferences refs = new ChildReferences();
    var flag = new Closeable();
    refs.addReference(flag);
    refs.close();
    assertThat(flag.closed).isTrue();
  }

  @Test
  void childReferencesIsWeak() throws Exception {
    ChildReferences refs = new ChildReferences();
    var flag = new Closeable();
    refs.addReference(flag);
    var ref = new WeakReference<>(flag);
    //noinspection UnusedAssignment
    flag = null;

    for (int i = 0; i < 50; i++) {
      System.gc();
      if (ref.get() == null) {
        break;
      }
      Thread.sleep(100);
    }

    assertThat(ref.get()).isNull();
    refs.close();
  }

  static final class Closeable implements AutoCloseable {
    boolean closed = false;

    @Override
    public void close() throws Exception {
      closed = true;
    }
  }
}
