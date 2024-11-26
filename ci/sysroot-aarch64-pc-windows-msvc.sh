#!/bin/sh

# https://github.com/mstorsjo/msvc-wine/blob/master/vsdownload.py
# https://github.com/mozilla/gecko-dev/blob/6027d1d91f2d3204a3992633b3ef730ff005fc64/build/vs/vs2022-car.yaml

# function dl() {
# 	curl -O https://download.visualstudio.microsoft.com/download/pr/$1
# }

# [[.h]]

# "id": "Win11SDK_10.0.26100"
# "version": "10.0.26100.7"

# libucrt.lib

# example: <assert.h>
# dir: ucrt/
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/2ee3a5fc6e9fc832af7295b138e93839/universal%20crt%20headers%20libraries%20and%20sources-x86_en-us.msi
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/b1aa09b90fe314aceb090f6ec7626624/16ab2ea2187acffa6435e334796c8c89.cab
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/400609bb0ff5804e36dbe6dcd42a7f01/6ee7bbee8435130a869cf971694fd9e2.cab
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/2ac327317abb865a0e3f56b2faefa918/78fa3c824c2c48bd4a49ab5969adaaf7.cab
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/f034bc0b2680f67dccd4bfeea3d0f932/7afc7b670accd8e3cc94cfffd516f5cb.cab
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/7ed5e12f9d50f80825a8b27838cf4c7f/96076045170fe5db6d5dcf14b6f6688e.cab
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/764edc185a696bda9e07df8891dddbbb/a1e2a83aa8a71c48c742eeaff6e71928.cab
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/66854bedc6dbd5ccb5dd82c8e2412231/b2f03f34ff83ec013b9e45c7cd8e8a73.cab

# example: <windows.h>
# dir: um/
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/b286efac4d83a54fc49190bddef1edc9/windows%20sdk%20for%20windows%20store%20apps%20headers-x86_en-us.msi
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/e0dc3811d92ab96fcb72bf63d6c08d71/766c0ffd568bbb31bf7fb6793383e24a.cab
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/613503da4b5628768497822826aed39f/8125ee239710f33ea485965f76fae646.cab

# example: <winapifamily.h>
# dir: /shared
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/122979f0348d3a2a36b6aa1a111d5d0c/windows%20sdk%20for%20windows%20store%20apps%20headers%20onecoreuap-x86_en-us.msi
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/766e04beecdfccff39e91dd9eb32834a/e89e3dcbb016928c7e426238337d69eb.cab


# "id": "Microsoft.VisualC.14.16.CRT.Headers"
# "version": "14.16.27045"

# example: <vcruntime.h>
# dir: MSVC/
curl -O https://download.visualstudio.microsoft.com/download/pr/bac0afd7-cc9e-4182-8a83-9898fa20e092/87bbe41e09a2f83711e72696f49681429327eb7a4b90618c35667a6ba2e2880e/Microsoft.VisualC.14.16.CRT.Headers.vsix

# [[.lib]]

# advapi32.lib bcrypt.lib kernel32.lib ntdll.lib user32.lib uuid.lib ws2_32.lib userenv.lib cfgmgr32.lib runtimeobject.lib
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/944c4153b849a1f7d0c0404a4f1c05ea/windows%20sdk%20for%20windows%20store%20apps%20libs-x86_en-us.msi
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/5306aed3e1a38d1e8bef5934edeb2a9b/05047a45609f311645eebcac2739fc4c.cab
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/13c8a73a0f5a6474040b26d016a26fab/13d68b8a7b6678a368e2d13ff4027521.cab
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/149578fb3b621cdb61ee1813b9b3e791/463ad1b0783ebda908fd6c16a4abfe93.cab
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/5c986c4f393c6b09d5aec3b539e9fb4a/5a22e5cde814b041749fb271547f4dd5.cab

# fwpuclnt.lib arm64rt.lib
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/7a332420d812f7c1d41da865ae5a7c52/windows%20sdk%20desktop%20libs%20arm64-x86_en-us.msi
curl -O https://download.visualstudio.microsoft.com/download/pr/32863b8d-a46d-4231-8e84-0888519d20a9/19de98ed4a79938d0045d19c047936b3/3e2f7be479e3679d700ce0782e4cc318.cab

# libcmt.lib libvcruntime.lib
curl -O https://download.visualstudio.microsoft.com/download/pr/bac0afd7-cc9e-4182-8a83-9898fa20e092/227f40682a88dc5fa0ccb9cadc9ad30af99ad1f1a75db63407587d079f60d035/Microsoft.VisualC.14.16.CRT.ARM64.Desktop.vsix


msiextract universal%20crt%20headers%20libraries%20and%20sources-x86_en-us.msi
msiextract windows%20sdk%20for%20windows%20store%20apps%20headers-x86_en-us.msi
msiextract windows%20sdk%20for%20windows%20store%20apps%20headers%20onecoreuap-x86_en-us.msi
msiextract windows%20sdk%20for%20windows%20store%20apps%20libs-x86_en-us.msi
msiextract windows%20sdk%20desktop%20libs%20arm64-x86_en-us.msi
unzip -o Microsoft.VisualC.14.16.CRT.Headers.vsix
unzip -o Microsoft.VisualC.14.16.CRT.ARM64.Desktop.vsix

mkdir -p /usr/aarch64-pc-windows-msvc/usr/include
mkdir -p /usr/aarch64-pc-windows-msvc/usr/lib

# lowercase folder/file names
echo "$(find . -regex ".*/[^/]*[A-Z][^/]*")" | xargs -I{} sh -c 'mv "$(echo "{}" | sed -E '"'"'s/(.*\/)/\L\1/'"'"')" "$(echo "{}" | tr [A-Z] [a-z])"'

# .h
(cd 'program files/windows kits/10/include/10.0.26100.0' && cp -r ucrt/* um/* shared/* -t /usr/aarch64-pc-windows-msvc/usr/include)

cp -r contents/vc/tools/msvc/14.16.27023/include/* /usr/aarch64-pc-windows-msvc/usr/include

# lowercase #include "" and #include <>
find /usr/aarch64-pc-windows-msvc/usr/include -type f -exec sed -i -E 's/(#include <[^<>]*?[A-Z][^<>]*?>)|(#include "[^"]*?[A-Z][^"]*?")/\L\1\2/' "{}" ';'

# ARM intrinsics
# original dir: MSVC/

# '__n128x4' redefined in arm_neon.h
# "arm64_neon.h" included from intrin.h

(cd /usr/lib/llvm19/lib/clang/19/include && cp arm_neon.h intrin.h -t /usr/aarch64-pc-windows-msvc/usr/include)

# .lib

# _Interlocked intrinsics
# must always link with arm64rt.lib
# reason: https://developercommunity.visualstudio.com/t/libucrtlibstreamobj-error-lnk2001-unresolved-exter/1544787#T-ND1599818
# I don't understand the 'correct' fix for this, arm64rt.lib is supposed to be the workaround

(cd 'program files/windows kits/10/lib/10.0.26100.0/um/arm64' && cp advapi32.lib bcrypt.lib kernel32.lib ntdll.lib user32.lib uuid.lib ws2_32.lib userenv.lib cfgmgr32.lib runtimeobject.lib fwpuclnt.lib arm64rt.lib -t /usr/aarch64-pc-windows-msvc/usr/lib)

(cd 'contents/vc/tools/msvc/14.16.27023/lib/arm64' && cp libcmt.lib libvcruntime.lib -t /usr/aarch64-pc-windows-msvc/usr/lib)

cp 'program files/windows kits/10/lib/10.0.26100.0/ucrt/arm64/libucrt.lib' /usr/aarch64-pc-windows-msvc/usr/lib