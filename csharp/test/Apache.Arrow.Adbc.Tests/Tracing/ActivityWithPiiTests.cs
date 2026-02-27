using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tracing;
using Xunit;

namespace Apache.Arrow.Adbc.Testing.Tracing
{

    public class ActivityWithPiiTests
    {
        private class RedactedValueTestData : TheoryData<object?, object?>
        {
            public RedactedValueTestData()
            {
                Add(null, null);
                Add((sbyte)1, (sbyte)1);
                Add((byte)1, (byte)1);
                Add((short)1, (short)1);
                Add((ushort)1, (ushort)1);
                Add((int)1, (int)1);
                Add((uint)1, (uint)1);
                Add((long)1, (long)1);
                Add((ulong)1, (ulong)1);
                Add((decimal)1, (decimal)1);
                Add((float)1, (float)1);
                Add((double)1, (double)1);
                Add(true, true);
                Add('A', 'A');
                Add("string", "string" );
                Add(new object?[] { null }, new object?[] { null });
                Add(new object?[] { (sbyte)1 }, new object?[] { (sbyte)1 });
                Add(new object?[] { (byte)1 }, new object?[] { (byte)1 });
                Add(new object?[] { (short)1 }, new object?[] { (short)1 });
                Add(new object?[] { (ushort)1 }, new object?[] { (ushort)1 });
                Add(new object?[] { (int)1 }, new object?[] { (int)1 });
                Add(new object?[] { (uint)1 }, new object?[] { (uint)1 });
                Add(new object?[] { (long)1 }, new object?[] { (long)1 });
                Add(new object?[] { (ulong)1 }, new object?[] { (ulong)1 });
                Add(new object?[] { (decimal)1 }, new object?[] { (decimal)1 });
                Add(new object?[] { (float)1 }, new object?[] { (float)1 });
                Add(new object?[] { (double)1 }, new object?[] { (double)1 });
                Add(new object?[] { true }, new object?[] { true });
                Add(new object?[] { 'A' }, new object?[] { 'A' });
                Add(new object?[] { "string" }, new object?[] { "string" });
            }
        }

        [SkippableTheory]
        [ClassData(typeof(RedactedValueTestData))]
        public void CanAddTagDefaultIsPii(object? testValue, object? expectedValue)
        {
            string activityName = NewName();
            Activity activity = new(activityName);
            var activityWithPii = ActivityWithPii.New(activity);

            Assert.NotNull(activityWithPii);
            activityWithPii.AddTag("keyName", testValue);
            var value = activity.TagObjects.First().Value as RedactedValue;
            Assert.NotNull(value);
            Assert.Equal(expectedValue, value.GetValue());
            Assert.Equal(RedactedValue.DefaultValue, value.ToString());
        }

        [SkippableFact]
        public void CanAddTagUsingDelegateDefaultIsPii()
        {
            List<(object? TestValue, object? ExpectedValue)> testData = [];
            testData.Add((() => new object?[] { "string" }, new object?[] { "string" }));
            testData.Add((() => new object?[] { 1 }, new object?[] { 1 }));
            testData.Add((() => new object?[] { null }, new object?[] { null }));
            testData.Add((getValue(1), "1"));
            testData.Add((getAnotherValue(getValue(1)), "1"));

            static Func<string> getAnotherValue(Func<string> anotherFunction) => () => anotherFunction();
            static Func<string> getValue(int localValue) => localValue.ToString;

            string activityName = NewName();
            Activity activity = new(activityName);
            var activityWithPii = ActivityWithPii.New(activity);
            int index = 0;
            Assert.NotNull(activityWithPii);

            foreach ((object? testValue, object? expectedValue) in testData)
            {
                string key = "keyName" + index++;
                activityWithPii = activityWithPii.AddTag(key, testValue);
                Assert.Contains(activity.TagObjects, e => e.Key == key);
                var value = activity.TagObjects.Where(e => e.Key == key).First().Value as RedactedValue;
                Assert.NotNull(value);
                Assert.Equal(expectedValue, value.GetValue());
                Assert.Equal(RedactedValue.DefaultValue, value.ToString());
            }
        }

        [SkippableTheory]
        [ClassData(typeof(RedactedValueTestData))]
        public void CanAddTagIsPiiAsFalse(object? testValue, object? expectedValue)
        {
            string activityName = NewName();
            Activity activity = new(activityName);
            var activityWithPii = ActivityWithPii.New(activity);
            Assert.NotNull(activityWithPii);

            activityWithPii.AddTag("keyName", testValue, isPii: false);
            object? value = activity.TagObjects.First().Value;
            if (expectedValue == null)
            {
                Assert.Null(value);
            }
            else
            {
                Assert.NotNull(value);
                Assert.IsNotType<RedactedValue>(value);
            }
            Assert.Equal(expectedValue, value);
        }

        [SkippableFact]
        public void CanAddTagUsingDelegateIsPiiFalse()
        {
            List<(object? TestValue, object? ExpectedValue)> testData = [];
            testData.Add((() => new object?[] { "string" }, new object?[] { "string" }));
            testData.Add((() => new object?[] { 1 }, new object?[] { 1 }));
            testData.Add((() => new object?[] { null }, new object?[] { null }));
            testData.Add((getValue(1), "1"));
            testData.Add((getAnotherValue(getValue(1)), "1"));

            static Func<string> getAnotherValue(Func<string> anotherFunction) => () => anotherFunction();
            static Func<string> getValue(int localValue) => localValue.ToString;

            string activityName = NewName();
            Activity activity = new(activityName);
            var activityWithPii = ActivityWithPii.New(activity);
            int index = 0;
            Assert.NotNull(activityWithPii);

            foreach ((object? testValue, object? expectedValue) in testData)
            {
                string key = "keyName" + index++;
                activityWithPii = activityWithPii.AddTag(key, testValue, false);
                Assert.Contains(activity.TagObjects, e => e.Key == key);
                object? value = activity.TagObjects.Where(e => e.Key == key).First().Value;
                Assert.NotNull(value);
                Assert.IsNotType<RedactedValue>(value);
                Assert.Equal(expectedValue, value);
            }
        }

        private static string NewName() => new Guid().ToString("N");
    }
}
