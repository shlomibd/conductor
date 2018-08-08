/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * 
 */
package com.netflix.conductor.server.resources.v2;

import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.core.events.EventProcessor;
import com.netflix.conductor.core.events.EventQueues;
import com.netflix.conductor.service.MetadataService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;


/**
 * @author Viren
 *
 */
@Api(value="/v2/event", produces=MediaType.APPLICATION_JSON, consumes=MediaType.APPLICATION_JSON, tags="Event Services")
@Path("/v2/event")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Singleton
public class EventResource {

    private MetadataService metadataService;

    private EventProcessor eventProcessor;

    @Inject
    public EventResource(MetadataService metadataService, EventProcessor eventProcessor) {
        this.metadataService = metadataService;
        this.eventProcessor = eventProcessor;
    }

    @POST
    @ApiOperation("Add a new event handler.")
    public void addEventHandler(EventHandler eventHandler) {
        metadataService.addEventHandler(eventHandler);
    }

    @PUT
    @ApiOperation("Update an existing event handler.")
    public void updateEventHandler(EventHandler eventHandler) {
        metadataService.updateEventHandler(eventHandler);
    }


    @DELETE
    @Path("/{name}")
    @ApiOperation("Remove an event handler")
    public void removeEventHandlerStatus(@PathParam("name") String name) {
        metadataService.removeEventHandlerStatus(name);
    }

    @GET
    @ApiOperation("Get all the event handlers")
    public List<EventHandler> getEventHandlers() {
        return metadataService.getEventHandlers();
    }

    @GET
    @Path("/{event}")
    @ApiOperation("Get event handlers for a given event")
    public List<EventHandler> getEventHandlersForEvent(
            @PathParam("event") String event, @QueryParam("activeOnly") @DefaultValue("true") boolean activeOnly) {
        return metadataService.getEventHandlersForEvent(event, activeOnly);
    }

    @GET
    @Path("/queues")
    @ApiOperation("Get registered queues")
    public Map<String, ?> getEventQueues(@QueryParam("verbose") @DefaultValue("false") boolean verbose) {
        return (verbose ? eventProcessor.getQueueSizes() : eventProcessor.getQueues());
    }

    @GET
    @Path("/queues/providers")
    @ApiOperation("Get registered queue providers")
    public List<String> getEventQueueProviders() {
        return EventQueues.providers();
    }
}
