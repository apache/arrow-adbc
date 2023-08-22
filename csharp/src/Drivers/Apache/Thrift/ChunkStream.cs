using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.Drivers.Apache
{
    internal class ChunkStream : Stream
    {
        ReadOnlyMemory<byte> currentBuffer;
        byte[] data;
        bool first;
        int position;

        public ChunkStream(Schema schema, byte[] data)
        {
            MemoryStream buffer = new MemoryStream();
            ArrowStreamWriter writer = new ArrowStreamWriter(buffer, schema, leaveOpen: true);
            writer.WriteStart();
            writer.WriteEnd();
            writer.Dispose();

            this.currentBuffer = new ReadOnlyMemory<byte>(buffer.GetBuffer(), 0, (int)buffer.Length - 8);
            this.data = data;
            this.first = true;
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int remaining = this.currentBuffer.Length - this.position;
            if (remaining == 0)
            {
                if (this.first)
                {
                    this.first = false;
                }
                else
                {
                    return 0;
                }
                this.currentBuffer = new ReadOnlyMemory<byte>(this.data);
                this.position = 0;
                remaining = this.currentBuffer.Length - this.position;
            }

            int bytes = Math.Min(remaining, count);
            this.currentBuffer.Slice(this.position, bytes).CopyTo(new Memory<byte>(buffer, offset, bytes));
            this.position += bytes;
            return bytes;
        }

        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return base.ReadAsync(buffer, offset, count, cancellationToken);
        }

        public override int ReadByte()
        {
            return base.ReadByte();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
    }
}
