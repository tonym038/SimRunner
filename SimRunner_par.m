function SimRunner_par(MAINFXN, RUNF, EXPNAME, SAVERESFXN)%, RESUME_SIMULATION)

% SIMRUNNER_PAR Run experiments in parallel.
%    SIMRUNNER_PAR(RUNF, EXPNAME, RESUME_SIMULATION) requires that the 
%    main file MAINFXN is on the path and, like 
%    'test_main.m', accepts a structure of inputs 'inputs'.  
% 
% Mandatory Inputs
%    MAINFXN is name of model file to run. E.g. 'test_main'
%    RUNF is the runfile containing the structured array CONSTANT and 
%       vectors of experimental conditions contained in structured array 
%       VARIABLE. E.g. 'test_runfile.txt'
%
% Optional Inputs
%    EXPNAME is the output .mat file name to save. Default: .mat version of
%       RUNF
%    SAVERESFXN is the name of function for customized saving of results 
%       (e.g. saving selective results or aggregating results before saving)
%    RESUME_SIMULATION: whether to pick up where previous run crashed.
%       Determines pick-up point from .mat file. If no mat file exists, will
%       run from beginning.
%
%    Saves the same structure as in SIMRUNNER. Parfor run in batches of
%    replicates of each parameter case. So intermediate results can be
%    saved if parpool crash
%
%       Example: run the test runfile in parallel
%          SimRunner_par('test_main', 'test_runfile.txt', 'test_out_par')
%
%    For more information on setup see the helpfile for SIMRUNNER.
%
%    Notes:
%       - requires Distributed Computing Toolbox.
%
% See also SIMRUNNER MAKE_EXPS_TABLE SMARTTIME PREDICT_TIME

% Author: SA, 2 Oct 2017 (Monash)
% Modified: TM, 31 Mar 2025 (Curtin)

    %% Execution Setup
time_start = datetime();
print_progress = true; % turn to false for silent simulation
print_period_short = minutes(10); % print after every parameter case until this period is reached
print_freq_short = seconds(1);
print_freq_long = hours(1); % after reaching this value, reduce print frequency to once every longterm period
print_freq_medium = print_period_short;
print_period_current = "short"; % tracks whether currently in short, medium, or long term print period
print_freq_current = print_freq_short;

   
    %% Checks
if ~exist('EXPNAME', 'var') || isempty(EXPNAME)
    EXPNAME = strrep(RUNF, '.txt', '.mat');
end
% if ~exist('RESUME_SIMULATION', 'var') || isempty(RESUME_SIMULATION') || ~exist(EXPNAME, 'file')
    % RESUME_SIMULATION = false;
% end
if ~exist('MAINFXN', 'var') || isempty(MAINFXN)
    MAINFXN = 'runModel';    
end

% .. check license
if license('checkout', 'distrib_computing_toolbox') == 0
    error('Did not find distributed computing toolbox installed.')
end

    %% Simulation Setup
% -- create function to run model
run_main = eval(['@(inputs) ' MAINFXN '(inputs)']);
% run_main = @(inputs) runModel(inputs);
    
% -- Ingest constants and variables
T = make_exps_table(RUNF);

% -- Setup
% Notes:
%  . to satisify variable transparency, we pre-create the summary structure
%  . in addition, we must enforce the main proc, 'runModel()' since we cannot use `eval()` since it breaks transparency
crash_limit = 5;
n_ex = height(T);
if ismember('n_replicates', T.Properties.VariableNames)
    n_rep = T{1, 'n_replicates'}; % assuming table sorted by RNG seed last
else
    n_rep = 1;
end

n_cases = n_ex / n_rep;
a = struct('inputs', [], 'results', [], 'start', [], 'duration', []);
summary_case_empty = repmat(a, n_rep, 1);
% if RESUME_SIMULATION % assuming M contains only completed simulations
    % load(EXPNAME);
    % case_start = height(summary) + 1;
% else
    case_start = 1;    
    summary = repmat(a, n_ex, 1);    
% end
if print_progress
    fprintf('Setup finished: %s\n', datetime() - time_start);
end

%% -- Run (parallel)
if print_progress
    fprintf('Starting short-term progress printing ...\n');
    time_last_print = datetime();
end
for i_case = case_start :n_cases % running in batches of replicates of each parameter case
    ex_range = (i_case - 1) * n_rep + 1 : i_case * n_rep; % range of case replicate indices in full set
    T_case = T(ex_range,:);
    summary_case = summary_case_empty; % reset summary_case
    
    if n_rep == 1 % skip parpool creation if only 1 replicate
        ex = 1;
        rng(1)              % for reproducibility
        % .. get inputs for this run
        inputs = table2struct(T_case(ex,:));
        % .. save inputs, evaluate model
        summary_case(ex).inputs  = inputs;
        summary_case(ex).start   = datetime;                
        % summary_case(ex).results = runModel(inputs);
        summary_case(ex).results = run_main(inputs);
        summary_case(ex).duration    = datetime() - summary_case(ex).start;
        crash_count = -1; % indicates successful run
    else
        crash_count = 0; % number of times parfor has crashed    
        while crash_count >= 0 && crash_count < crash_limit % retry upon error, but give up if re-occuring
            try % run parfor
                parfor ex = 1:n_rep
                    rng(1)              % for reproducibility
                    % .. get inputs for this run
                    inputs = table2struct(T_case(ex,:));
                    % .. save inputs, evaluate model
                    summary_case(ex).inputs  = inputs;
                    summary_case(ex).start   = datetime;                
                    % summary_case(ex).results = runModel(inputs);
                    summary_case(ex).results = run_main(inputs);
                    summary_case(ex).duration    = datetime() - summary_case(ex).start;
                end
            catch ME
                crash_count = crash_count + 1; 
                if print_progress
                    fprintf('Crash #%d: %s\n', crash_count, ME.identifier);
                end
                if crash_count == crash_limit % give up on trying to get it to run
                    fprintf('Unable to run without error.\n');
                    rethrow(ME)
                end
                if strcmp(ME.identifier, 'parallel:lang:parfor:SessionShutDown') % if error is due to parpool crash
                    delete(gcp('nocreate')); % shut down parpool
                    pause(2^crash_count); % gap between retries widens, in case crash is due to current RAM/CPU load
                    parpool(); % restart parpool
                    continue; % retry parfor           
                else
                    rethrow(ME) % if not expected parfor error, terminate MATLAB as per exception
                end
            end
            crash_count = -1; % indicates successful run
        end
    end
    summary(ex_range) = summary_case;
    
    if print_progress
        if print_period_current == "short" && datetime() - time_start > print_period_short
            fprintf('Switching to medium-term printing ...\n');
            print_period_current = "medium";
            print_freq_current = print_freq_medium;            
        end
        if print_period_current == "medium" && datetime() - time_start > print_freq_long
            fprintf('Switching to long-term printing ...\n');
            print_period_current = "long";
            print_freq_current = print_freq_long;            
        end
        if datetime() - time_last_print > print_freq_current            
            fprintf('\tParameter Case #%d/%d (%.0f%%) completed: %s\n', i_case, n_cases, i_case/n_cases*100 ... 
                , datetime());
            time_last_print = datetime();
        end
    end
end
if print_progress
    fprintf('Runs finished: %s\n', datetime() - time_start);
end

    %% .. finish up, save
if exist('AGGRESFXN', 'var') && ~isempty(SAVERESFXN)
    eval([SAVERESFXN '(EXPNAME, summary)']);
else
    save(EXPNAME,'summary');
    % save(EXPNAME,'summary','-v7.3'); % for large files
end

if print_progress
    fprintf('Program finished: %s\n', datetime - time_start);
end
