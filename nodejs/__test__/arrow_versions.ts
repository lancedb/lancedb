// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// Every Apache Arrow release the package accepts as a peer dependency, as
// separately installed copies so the tests can feed objects built by one
// Arrow version into a LanceDB running on another.  When a new Arrow major
// is released, add the alias to `package.json` and extend this list; the
// test files pick the change up from here.
import * as arrow15 from "apache-arrow-15";
import * as arrow16 from "apache-arrow-16";
import * as arrow17 from "apache-arrow-17";
import * as arrow18 from "apache-arrow-18";
import * as arrow19 from "apache-arrow-19";
import * as arrow20 from "apache-arrow-20";
import * as arrow21 from "apache-arrow-21";

export { arrow15, arrow16, arrow17, arrow18, arrow19, arrow20, arrow21 };

export type ApacheArrow =
  | typeof arrow15
  | typeof arrow16
  | typeof arrow17
  | typeof arrow18
  | typeof arrow19
  | typeof arrow20
  | typeof arrow21;

export const arrowVersions: ApacheArrow[] = [
  arrow15,
  arrow16,
  arrow17,
  arrow18,
  arrow19,
  arrow20,
  arrow21,
];

/** The newest supported Arrow release, for tests that only need one. */
export const latestArrow = arrow21;
