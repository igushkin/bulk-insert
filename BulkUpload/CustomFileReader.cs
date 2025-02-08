using System.Text;

namespace BulkUpload
{
    class CustomFileReader(String path, int minWordLength, int maxWordLength) : IDisposable
    {
        private readonly StreamReader reader = new(path);
        private readonly StringBuilder stringBuilder = new();
        private readonly int minLength = minWordLength;
        private readonly int maxLength = maxWordLength;

        public String? ReadNextWord()
        {
            while (!reader.EndOfStream)
            {
                var word = NextWord();
                if (word.Length >= minLength && word.Length <= maxLength)
                {
                    return word;
                }
            }
            return null;
        }

        private String NextWord()
        {
            SkipSpaces();
            stringBuilder.Clear();

            while (!reader.EndOfStream)
            {
                var ch = (char)reader.Read();
                if (ch == ' ' || Environment.NewLine.Contains(ch)) break;
                if (stringBuilder.Length <= maxLength) stringBuilder.Append(ch);
            }
            return stringBuilder.ToString();
        }

        private void SkipSpaces()
        {
            while ((char)reader.Peek() == ' ') reader.Read();
        }

        public void Dispose()
        {
            reader.Dispose();
        }
    }
}
