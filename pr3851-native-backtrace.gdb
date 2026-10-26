set pagination off
set debuginfod enabled off
set print frame-arguments none
info threads
thread apply all bt 24
bt 48
info registers rip rsp
info proc mappings
frame 4
disassemble
frame 3
info registers rdi rsi rax rdx rip
x/6i $pc
x/gx $rdi
