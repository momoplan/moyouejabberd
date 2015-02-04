-ifndef(_aa_inf_types_included).
-define(_aa_inf_types_included, yeah).

%% struct aaRequest

-record(aaRequest, {sn :: string() | binary(),
                    content :: string() | binary(),
                    type = "xml" :: string() | binary()}).

-endif.
