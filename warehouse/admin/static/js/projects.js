/* SPDX-License-Identifier: Apache-2.0 */

import { columnVisibilityMenu } from "./tabulator";
import { linkCell, remoteTable } from "./utils/remote_table";

const element = document.getElementById("projects-table");
if (element) {
  const columns = [
    {
      title: "Project name",
      field: "name",
      formatter: linkCell("detail_link"),
      headerFilter: "input",
      headerFilterPlaceholder: "name fragment or uuid, then Enter",
      // Applied on Enter, unlike the users and organizations tables: a one
      // or two character fragment cannot use the trigram index and scans
      // close to a million names, about two seconds per pause while typing.
      headerFilterLiveFilter: false,
      headerSort: true,
    },
    {
      title: "Normalized name",
      field: "normalized_name",
    },
  ];

  // Allow deep-linking the name filter, e.g. /admin/projects/?name=struct
  const query = new URLSearchParams(window.location.search);
  const initialHeaderFilter = columns
    .map((column) => column.field)
    .filter((field) => query.get(field))
    .map((field) => ({ field: field, value: query.get(field) }));

  remoteTable(element, {
    placeholder: "No matching projects",
    initialSort: [{ column: "name", dir: "asc" }],
    initialHeaderFilter: initialHeaderFilter,
    columnDefaults: {
      headerSort: false,
      headerMenu: columnVisibilityMenu,
    },
    columns: columns,
  });
}
