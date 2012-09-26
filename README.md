couchbase-aspnet
================

This library provides infrastructure support for using [Couchbase Server](http://couchbase.com) and ASP.NET.

## Features:

ASP.NET SessionState Provider

* Port of the [Enyim Memcached Provider](https://github.com/enyim/memcached-providers) to Couchbase Server

## Requirements

* You'll need .NET Framework 3.5 or later to use the precompiled binaries. 
* To build the client, you'll need Visual Studio 2010 with MVC 3 installed.
* The Nuget package for [CouchbaseNetClient 1.1.6](http://nuget.org/packages/CouchbaseNetClient) is referenced by Couchbase.AspNet
* Couchbase Server 1.8

## Configuring the SessionState provider

Update the sessionState section in Web.config as follows:

    <sessionState customProvider="Couchbase" mode="Custom">
      <providers>
        <add name="Couchbase" type="Couchbase.AspNet.SessionState.CouchbaseSessionStateProvider, Couchbase.AspNet" />
      </providers>
    </sessionState>
		
Configure the Couchbase Client as you normally would:

    <section name="couchbase" type="Couchbase.Configuration.CouchbaseClientSection, Couchbase"/>	

	<couchbase>
		<servers bucket="default" bucketPassword="">
		<add uri="http://127.0.0.1:8091/pools"/>      
		</servers>
	</couchbase>

If you would like to use a custom configuration section, you may do so by specifying a value for the "section" attribute of the provider entry (see below).

    <section name="couchbaseSession" type="Couchbase.Configuration.CouchbaseClientSection, Couchbase"/>    

	<couchbaseSession>
		<servers bucket="sessionState" bucketPassword="">
		<add uri="http://127.0.0.1:8091/pools"/>      
		</servers>
	</couchbaseSession>

    <sessionState customProvider="Couchbase" mode="Custom">
      <providers>
        <add name="Couchbase" type="Couchbase.AspNet.SessionState.CouchbaseSessionStateProvider, Couchbase.AspNet" section="couchbaseSession" />
      </providers>
    </sessionState>

If you would like to use a custom client factory, you may do so by specifying a value in the "factory" attribute of the provider entry. The example below sets it to the default factory, but you can replace this with your own factory class to have full control over the creation and lifecycle of the Couchbase client.

    <sessionState customProvider="Couchbase" mode="Custom">
      <providers>
        <add name="Couchbase" type="Couchbase.AspNet.SessionState.CouchbaseSessionStateProvider, Couchbase.AspNet" factory="Couchbase.AspNet.SessionState.CouchbaseClientFactory" />
      </providers>
    </sessionState>

This session handler also supports the ability to disable exclusive session access for ASP.NET sessions if desired. You can set the value using the "exclusiveAccess" attribute of the provider entry.

    <sessionState customProvider="Couchbase" mode="Custom">
      <providers>
        <add name="Couchbase" type="Couchbase.AspNet.SessionState.CouchbaseSessionStateProvider, Couchbase.AspNet" exclusiveAccess="false" />
      </providers>
    </sessionState>
	
Note that currently, code-based configuration of the CouchbaseClient is not supported.

In code, simply use the Session object as you normally would.

	Session["Message"] = "Couchbase is awesome!";

Be sure to mark any user defined types as Serializable.

	[Serializable]
	public class SessionUser 
	{
		public string Username { get; set; }

		public string Email { get; set; }
	}

## Configuring the OutputCache provider

Update the outputCache section in Web.config as follows:

    <outputCache defaultProvider="CouchbaseCache">
		<providers>
			<add name="CouchbaseCache" type="Couchbase.AspNet.OutputCache.CouchbaseOutputCacheProvider, Couchbase.AspNet" section="couchbase-caching"/>
		</providers>
    </outputCache>

Configure the Couchbase Client as you normally would:

    <section name="couchbase" type="Couchbase.Configuration.CouchbaseClientSection, Couchbase"/>

	<couchbase>
		<servers bucket="default" bucketPassword="">
			<add uri="http://127.0.0.1:8091/pools"/>
		</servers>
	</couchbase>

If you would like to use a custom configuration section, you may do so by specifying a value for the "section" attribute of the provider entry (see below).

    <section name="couchbaseSession" type="Couchbase.Configuration.CouchbaseClientSection, Couchbase"/>

	<couchbaseSession>
		<servers bucket="sessionState" bucketPassword="">
			<add uri="http://127.0.0.1:8091/pools"/>
		</servers>
	</couchbaseSession>

    <outputCache defaultProvider="CouchbaseCache">
      <providers>
        <add name="Couchbase" type="Couchbase.AspNet.SessionState.CouchbaseSessionStateProvider, Couchbase.AspNet" section="couchbaseSession" />
      </providers>
    </outputCache>

Once configured, simply enable output cache as you already do with ASP.NET MVC

    [OutputCache(Duration = 60, VaryByParam="foo")]
    public ActionResult Time(string foo)
    {
    	return Content(DateTime.Now.ToString());
    }

or with ASP.NET WebForms

    <%@ OutputCache Duration="60" VaryByParam="foo" %>