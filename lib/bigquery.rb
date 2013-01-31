require 'google/api_client'

class BigQuery

  attr_accessor :dataset, :project_id

  def initialize(opts = {})
    @client = Google::APIClient.new

    key = Google::APIClient::PKCS12.load_key(
      opts['key'],
      "notasecret"
    )

    @asserter = Google::APIClient::JWTAsserter.new(
      opts['service_email'],
      "https://www.googleapis.com/auth/bigquery",
      key
    )

    refresh_auth

    @bq = @client.discovered_api("bigquery", "v2")

    @project_id = opts['project_id']
    @dataset = opts['dataset']
  end

  # https://developers.google.com/bigquery/docs/queries#syncqueries
  def query(q)
    api({
      :api_method => @bq.jobs.query,
      :body_object => { "query" => q, 'timeoutMs' => 90 * 1000}
    })
  end

  # https://developers.google.com/bigquery/docs/queries#syncqueries
  def query!(*args)
    res = query(*args)
    raise SynchronousQueryError.new(res) if res.has_key? "error"
    res
  end

  # https://developers.google.com/bigquery/docs/reference/v2/jobs#querying
  # https://developers.google.com/bigquery/docs/queries#asyncqueries
  def asynchronous_query(q, opts = {})
    opts = {
      "query" => q,
      "priority" => "INTERACTIVE",
    }.merge(opts)

    api({
      :api_method => @bq.jobs.query,
      :body_object => {
        "configuration" => {
          "query" => opts
        }
      }
    })
  end

  # https://developers.google.com/bigquery/docs/reference/v2/jobs#querying
  # https://developers.google.com/bigquery/docs/queries#asyncqueries
  def asynchronous_query!(*args)
    asynchronous_query(*args)
    raise_if_job_error(res)
    res
  end

  # https://developers.google.com/bigquery/docs/reference/v2/jobs#querying
  # https://developers.google.com/bigquery/docs/queries#batchqueries
  def asynchronous_batch_query(q, opts = {})
    asynchronous_query(q, opts.merge("priority" => "BATCH"))
  end

  # https://developers.google.com/bigquery/docs/reference/v2/jobs#querying
  # https://developers.google.com/bigquery/docs/queries#batchqueries
  def asynchronous_batch_query!(*args)
    res = asynchronous_batch_query(*args)
    raise_if_job_error(res)
    res
  end

  # https://developers.google.com/bigquery/docs/reference/v2/jobs#querying#importing
  def load(opts)
    api({
      :api_method => @bq.jobs.insert,
      :body_object => {
        "configuration" => {
          "load" => opts
        }
      }
    })
  end

  # https://developers.google.com/bigquery/docs/reference/v2/jobs#importing
  def load!(*args)
    res = load(*args)
    raise_if_job_error(res)
    res
  end

  # https://developers.google.com/bigquery/docs/reference/v2/jobs/get
  def job(id, opts = {})
    # Querying a nil ID, will make BigQuery list all jobs
    raise ArgumentError, 'Cannot get Job details with a nil id' if id == nil

    opts['jobId'] = id

    api({
      :api_method => @bq.jobs.get,
      :parameters => opts
    })
  end

  # https://developers.google.com/bigquery/docs/reference/v2/jobs/get
  def job!(*args)
    res = job(*args)
    raise_if_job_error(res)
    res
  end

  # https://developers.google.com/bigquery/docs/reference/v2/jobs/list
  def jobs(opts = {})
    api({
      :api_method => @bq.jobs.list,
      :parameters => opts
    })
  end

  # https://developers.google.com/bigquery/docs/reference/v2/jobs/getQueryResults
  def get_query_results(jobId, opts = {})
    opts['jobId'] = jobId
    api({
      :api_method => @bq.jobs.get_query_results,
      :parameters => opts
    })
  end

  # perform a query synchronously
  # fetch all result rows, even when that takes >1 query
  # invoke /block/ once for each row, passing the row
  def each_row(q, &block)
    current_row = 0
    # repeatedly fetch results, starting from current_row
    # invoke the block on each one, then grab next page if there is one
    # it'll terminate when res has no 'rows' key or when we've done enough rows
    # perform query...
    res = query(q)
    job_id = res['jobReference']['jobId']
    # call the block on the first page of results
    if( res && res['rows'] )
      res['rows'].each(&block)
      current_row += res['rows'].size
    end
    # keep grabbing pages from the API and calling the block on each row
    while(( res = get_query_results(job_id, :startIndex => current_row) ) && res['rows'] && current_row < res['totalRows'].to_i ) do
      res['rows'].each(&block)
      current_row += res['rows'].size
    end
  end

  def tables(dataset = @dataset)
    api({
      :api_method => @bq.tables.list,
      :parameters => {"datasetId" => dataset}
    })['tables']
  end

  def tables_formatted(dataset = @dataset)
    tables(dataset).map {|t| "[#{dataset}.#{t['tableReference']['tableId']}]"}
  end

  def refresh_auth
    @client.authorization = @asserter.authorize
  end

  private

  def api(opts)
    if opts[:parameters]
      opts[:parameters] = opts[:parameters].merge({"projectId" => @project_id})
    else
      opts[:parameters] = {"projectId" => @project_id}
    end

    resp = @client.execute(opts)
    JSON.parse(resp.body)
  end

  def raise_if_job_error(res)
    if res['status']['errorResult']
      raise JobError.new(res)
    end
  end

  public

  class Error < StandardError; end
  class SynchronousQueryError < Error
    attr_reader :code
    def initialize(res)
      super(res['error']['message'])
      @code = res['error']['code']
    end
  end
  class JobError < Error
    attr_reader :reason, :errors
    def initialize(res)
      super(res['status']['errorResult']['message'])
      @reason = res['status']['errorResult']['reason']
      @errors = res['status']['errorResult']['errors']
    end
  end

end
