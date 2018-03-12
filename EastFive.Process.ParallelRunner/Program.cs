using System;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EastFive.Process.ParallelRunner
{
    public class Program
    {
        private static readonly string ExecutablePath;
        private static readonly int InstanceCount;
        private static readonly int MaxMinutesToRun;
        private static readonly int MinMinutesToRestart;
        private static readonly int MaxMinutesToRestart;
        private static readonly TimeSpan StartNewProcessesAfter;
        private static readonly TimeSpan DoNotStartNewProcessesAfter;

        static Program()
        {
            ExecutablePath = ConfigurationManager.AppSettings[nameof(ExecutablePath)];
            if (!File.Exists(ExecutablePath))
                throw new ArgumentException($"The {nameof(ExecutablePath)} of {ExecutablePath} does not exist!");
            if (!Int32.TryParse(ConfigurationManager.AppSettings[nameof(InstanceCount)], out InstanceCount) || InstanceCount < 1)
                throw new ArgumentException($"The {nameof(InstanceCount)} of {InstanceCount} must be a least 1.");
            if (!Int32.TryParse(ConfigurationManager.AppSettings[nameof(MaxMinutesToRun)], out MaxMinutesToRun) || MaxMinutesToRun < 15)
                throw new ArgumentException($"The {nameof(MaxMinutesToRun)} of {MaxMinutesToRun} must be a least 15.");
            if (!Int32.TryParse(ConfigurationManager.AppSettings[nameof(MinMinutesToRestart)], out MinMinutesToRestart) || MinMinutesToRestart < 5)
                throw new ArgumentException($"The {nameof(MinMinutesToRestart)} of {MinMinutesToRestart} must be a least 5.");
            if (!Int32.TryParse(ConfigurationManager.AppSettings[nameof(MaxMinutesToRestart)], out MaxMinutesToRestart) || MaxMinutesToRestart < 15)
                throw new ArgumentException($"The {nameof(MaxMinutesToRestart)} of {MaxMinutesToRestart} must be a least 15.");
            TimeSpan.TryParse(ConfigurationManager.AppSettings[nameof(StartNewProcessesAfter)], out StartNewProcessesAfter);
            TimeSpan.TryParse(ConfigurationManager.AppSettings[nameof(DoNotStartNewProcessesAfter)], out DoNotStartNewProcessesAfter);
            if (StartNewProcessesAfter == DoNotStartNewProcessesAfter && StartNewProcessesAfter != default(TimeSpan))
                throw new ArgumentException($"{nameof(StartNewProcessesAfter)} and {nameof(DoNotStartNewProcessesAfter)} cannot both contain the same value.");
        }

        public static void Main(string[] args)
        {
            var source = new CancellationTokenSource();
            var tasks = Enumerable.Range(1, InstanceCount).Select(x => (Action)(() => new Program().RunFromCmd(source)));
            Parallel.Invoke(new ParallelOptions
            {
                CancellationToken = source.Token,
                MaxDegreeOfParallelism = InstanceCount
            },tasks.ToArray());
        }

        private void WaitUntilTimeOfDayToRun()
        {
            if (StartNewProcessesAfter == DoNotStartNewProcessesAfter && StartNewProcessesAfter == default(TimeSpan))
                return;

            var now = DateTime.Now;
            var earlier = now.Date + StartNewProcessesAfter;
            var later = now.Date + DoNotStartNewProcessesAfter;
            if (StartNewProcessesAfter > DoNotStartNewProcessesAfter)
                earlier = earlier.AddDays(-1);

            var waitTime = new TimeSpan();
            if (now < earlier)
                waitTime = earlier - now;
            else if (now > later)
                waitTime = earlier.AddDays(1) - now;
            if (waitTime.TotalSeconds > 0)
            {
                Console.WriteLine($"Sleeping...will wake at {now + waitTime}");
                Thread.Sleep(waitTime);
            }
        }

        private void RunFromCmd(CancellationTokenSource source)
        {
            var noWorkCycles = 0;
            do
            {
                WaitUntilTimeOfDayToRun();
                try
                {
                    using (var process = new System.Diagnostics.Process())
                    {
                        process.StartInfo.FileName = ExecutablePath;
                        process.StartInfo.WorkingDirectory = Path.GetDirectoryName(ExecutablePath);
                        process.StartInfo.UseShellExecute = false;
                        process.StartInfo.CreateNoWindow = false;
                        process.Start();
                        Console.WriteLine($"{process.Id} started at {process.StartTime}.");

                        if (!process.WaitForExit(
                            Convert.ToInt32(TimeSpan.FromMinutes(MaxMinutesToRun).TotalMilliseconds)) || !process.HasExited)
                        {
                            Console.WriteLine($"Process {process.Id} had to be killed, check for incomplete data.");
                            process.Kill();
                        }
                        var workMinutes = (process.ExitTime - process.StartTime).TotalMinutes;
                        Console.WriteLine($"{process.Id} ended at {process.ExitTime}. [{workMinutes:F1} mins]");

                        // backoff multiples of MinMinutesToRestart when no work was done the previous run up to maximum of MaxMinutesToRestart
                        noWorkCycles = workMinutes > 5d ? 0 : noWorkCycles + 1;
                        var sleepMinutes = Math.Min(MinMinutesToRestart + (noWorkCycles * MinMinutesToRestart), MaxMinutesToRestart);
                        Console.WriteLine($"Sleeping...will wake at {DateTime.Now.AddMinutes(sleepMinutes)}");
                        Thread.Sleep(Convert.ToInt32(TimeSpan.FromMinutes(sleepMinutes).TotalMilliseconds));
                    }
                }
                catch (Exception ex)
                {
                    source.Cancel();
                    throw new Exception($"Executable {ExecutablePath} failed: ", ex);
                }
            } while (true);
        }
    }
}