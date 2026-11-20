$ErrorActionPreference = 'Stop'
$py = 'C:\Users\Manu\.espressif\python_env\idf6.1_py3.14_env\Scripts\python.exe'
$root = 'C:\Users\Manu\Documents\GitHub\macerun'
$out = 'C:\Users\Manu\AppData\Local\Temp\opencode\test_net_errors.exe'

& $py -m ziglang cc -target native -O0 `
    -I "$root\components\net\include" `
    "$root\test\host\test_net_errors.c" `
    "$root\components\net\net_errors.c" `
    -o $out

if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& $out
exit $LASTEXITCODE
