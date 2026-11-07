// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import { installLanceErrorTranslation } from "./errors";
import * as native from "./native.js";

// Side effect: every native method rejects with a LanceError subclass.
// `import *` can be a copy of the CommonJS exports, so install on both the
// namespace and the original module object. Class prototypes are shared;
// free functions are not.
installLanceErrorTranslation(native);
const original = (native as { default?: object }).default;
if (original != null && original !== native) {
  installLanceErrorTranslation(original);
}
