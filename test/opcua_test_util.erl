% SPDX-FileCopyrightText: 2019-2026 Peer Stritzinger GmbH <peer@stritzinger.com>
%
% SPDX-License-Identifier: Apache-2.0

-module(opcua_test_util).

% API
-export([without_error_logger/1]).

%--- API -----------------------------------------------------------------------

without_error_logger(Fun) ->
    error_logger:tty(false),
    try
        Fun()
    after
        error_logger:tty(true)
    end.
