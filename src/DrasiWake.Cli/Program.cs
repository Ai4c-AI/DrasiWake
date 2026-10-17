namespace DrasiWake.Cli;

public static class Program
{
    public static int Main(string[] args) =>
        DrasiWakeCli.Run(args, Console.Out, Console.Error);
}
