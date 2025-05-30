/**
 * Copyright (c) Small Trading Company Ltd (Destash.com).
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

using System;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace GraphlessDB.Analyzers
{
    [Generator]
    public class GraphlessDBSchemaIncrementalSourceGenerator : IIncrementalGenerator
    {
        public void Initialize(IncrementalGeneratorInitializationContext context)
        {
            GeneratorLogging.SetLoggingLevel(LoggingLevel.Fatal);
            // GeneratorLogging.SetLogFilePath("/Users/tom/GitHub/destash/src/csharp/GraphlessDB/csharp/GraphlessDBSchemaIncrementalSourceGeneratorLog.txt");
            try
            {
                context.RegisterPostInitializationOutput(static postInitializationContext =>
                {
                    // postInitializationContext.AddEmbeddedAttributeDefinition();
                    postInitializationContext.AddSource("GraphlessSchemaDBAttributes.g.cs", SourceText.From("""
                // <auto-generated/>

                #nullable enable annotations
                #nullable disable warnings

                // Suppress warnings
                #pragma warning disable CS0612, CS0618, CS9113, CS1591

                using System;
                using Microsoft.CodeAnalysis;

                namespace GraphlessDB
                {
                    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
                    internal sealed class GraphlessDBSchemaAttribute(string schema) : Attribute
                    {
                    }

                    internal static class ImmutableListExtensions
                    {
                        public static global::System.Collections.Immutable.ImmutableList<T> AddIf<T>(this global::System.Collections.Immutable.ImmutableList<T> source, bool predicate, T value)
                        {
                            if (!predicate) {
                                return source;
                            }

                            return source.Add(value);
                        }
                    }
                }
                """, Encoding.UTF8));
                });

                var graphNodeProvider = context
                    .SyntaxProvider
                    .ForAttributeWithMetadataName(
                        "GraphlessDB.GraphlessDBSchemaAttribute",
                        predicate: static (node, cancelToken) => HasGraphlessDBAttribute(node),
                        transform: static (ctx, cancelToken) => GetGraphSchema(ctx));

                context.RegisterSourceOutput(
                    graphNodeProvider,
                    (sourceProductionContext, source) => Generate(source, sourceProductionContext));
            }
            catch (Exception ex)
            {
                GeneratorLogging.LogMessage(ex.ToString(), LoggingLevel.Error);
                throw;
            }
        }

        private static bool HasGraphlessDBAttribute(SyntaxNode node)
        {
            try
            {
                GeneratorLogging.LogMessage($"HasGraphlessDBAttribute");
                return node is ClassDeclarationSyntax classDeclaration && classDeclaration
                    .AttributeLists
                    .SelectMany(a => a.Attributes)
                    .Where(IsGraphlessDBAttribute)
                    .Any();
            }
            catch (Exception ex)
            {
                GeneratorLogging.LogMessage(ex.ToString(), LoggingLevel.Error);
                throw;
            }
        }

        private static GraphSchema GetGraphSchema(GeneratorAttributeSyntaxContext ctx)
        {
            try
            {
                var attributeList = ctx.Attributes.ToList();
                GeneratorLogging.LogMessage($"GetGraphlessDBModels [{attributeList.Count} attributes]");
                return attributeList
                    .Where(IsGraphSchemaAttribute)
                    .Select(GetGraphSchema)
                    .Aggregate(GraphSchema.Empty, (acc, cur) => acc.Add(cur));
            }
            catch (Exception ex)
            {
                GeneratorLogging.LogMessage(ex.ToString(), LoggingLevel.Error);
                throw;
            }
        }

        private static bool IsGraphlessDBAttribute(AttributeSyntax attribute)
        {
            GeneratorLogging.LogMessage($"IsGraphlessDBAttribute");
            return attribute.Name.GetText().ToString() == "GraphlessDBSchema";
        }

        private static bool IsGraphSchemaAttribute(AttributeData attribute)
        {
            GeneratorLogging.LogMessage($"IsGraphlessDBAttribute");
            return attribute.AttributeClass?.Name == "GraphlessDBSchemaAttribute";
        }

        private static GraphSchema GetGraphSchema(AttributeData attribute)
        {
            GeneratorLogging.LogMessage($"GetGraphSchema");
            var schema = attribute.ConstructorArguments[0].Value?.ToString() ?? string.Empty;
            var namespaceValue = GetNamespace(attribute.ApplicationSyntaxReference?.GetSyntax());
            var name = GetName(attribute.ApplicationSyntaxReference?.GetSyntax());
            return GraphSchemaParser.Default.Parse(namespaceValue, name, schema);
        }

        private static string GetNamespace(SyntaxNode? syntaxNode)
        {
            while (syntaxNode != null)
            {
                if (syntaxNode is NamespaceDeclarationSyntax namespaceDeclarationSyntax)
                {
                    GeneratorLogging.LogMessage($"GetNamespace ({namespaceDeclarationSyntax.Name.ToString()})");
                    return namespaceDeclarationSyntax.Name.ToString();
                }

                syntaxNode = syntaxNode.Parent;
            }

            throw new InvalidOperationException("Namespace missing");
        }

        private static string GetName(SyntaxNode? syntaxNode)
        {
            while (syntaxNode != null)
            {
                if (syntaxNode is ClassDeclarationSyntax classDeclarationSyntax)
                {
                    GeneratorLogging.LogMessage($"GetName ({classDeclarationSyntax.Identifier})");
                    return classDeclarationSyntax.Identifier.ToString();
                }

                syntaxNode = syntaxNode.Parent;
            }

            throw new InvalidOperationException("Name missing");
        }

        private static void Generate(GraphSchema graph, SourceProductionContext context)
        {
            try
            {
                foreach (var entity in graph.Entities.Items)
                {
                    switch (entity)
                    {
                        case Node node:
                            GenerateNode(graph, node, context);
                            break;
                        case Edge edge:
                            GenerateEdge(graph, edge, context);
                            break;
                        default:
                            throw new NotSupportedException("Entity type not supported");
                    }
                }
                GenerateQueryablePropertyService(graph, context);
                GenerateEntityTypeExtensions(graph, context);
            }
            catch (Exception ex)
            {
                GeneratorLogging.LogMessage(ex.ToString(), LoggingLevel.Error);
                throw;
            }
        }

        private static void GenerateEntityTypeExtensions(GraphSchema graph, SourceProductionContext context)
        {
            var generatedString = new StringBuilder();
            generatedString.AppendLine("// <auto-generated/>");
            generatedString.AppendLine();
            generatedString.AppendLine("#nullable enable annotations");
            generatedString.AppendLine("#nullable disable warnings");
            generatedString.AppendLine();
            generatedString.AppendLine("// Suppress warnings about [Obsolete] member usage in generated code.");
            generatedString.AppendLine("#pragma warning disable CS0612, CS0618, CS9113, CS1591");
            generatedString.AppendLine();
            generatedString.AppendLine($"namespace {graph.Namespace}");
            generatedString.AppendLine("{");
            generatedString.AppendLine($"    public static class {graph.Name}GraphEntityTypeNativeServiceOptionsExtensions");
            generatedString.AppendLine("    {");
            generatedString.AppendLine($"        public static void Add{graph.Name}EntityTypeMappings(this global::GraphlessDB.GraphEntityTypeNativeServiceOptions source)");
            generatedString.AppendLine("        {");
            foreach (var entity in graph.Entities.Items.OrderBy(v => v.Name))
            {
                generatedString.AppendLine($"            source.TypeMappings.Add(nameof(global::{graph.Namespace}.{GetClassName(entity)}), typeof(global::{graph.Namespace}.{GetClassName(entity)}));");
            }
            generatedString.AppendLine("        }");
            generatedString.AppendLine("    }");
            generatedString.AppendLine("}");
            context.AddSource($"GraphEntityTypeNativeServiceOptionsExtensions.{graph.Name}.g.cs", generatedString.ToString());
        }


        private static void GenerateQueryablePropertyService(GraphSchema graph, SourceProductionContext context)
        {
            var generatedString = new StringBuilder();
            generatedString.AppendLine("// <auto-generated/>");
            generatedString.AppendLine();
            generatedString.AppendLine("#nullable enable annotations");
            generatedString.AppendLine("#nullable disable warnings");
            generatedString.AppendLine();
            generatedString.AppendLine("// Suppress warnings about [Obsolete] member usage in generated code.");
            generatedString.AppendLine("#pragma warning disable CS0612, CS0618, CS9113, CS1591");
            generatedString.AppendLine();
            generatedString.AppendLine($"namespace {graph.Namespace}");
            generatedString.AppendLine("{");
            generatedString.AppendLine($"    public sealed class {graph.Name}GraphQueryablePropertyService : global::GraphlessDB.Graph.Services.IGraphQueryablePropertyService");
            generatedString.AppendLine("    {");
            generatedString.AppendLine("        public bool IsQueryableProperty(string typeName, string propertyName)");
            generatedString.AppendLine("        {");
            generatedString.AppendLine("            return typeName switch");
            generatedString.AppendLine("            {");
            foreach (var entity in graph.Entities.Items.Where(e => !e.Properties.Items.IsEmpty))
            {
                generatedString.AppendLine($"                \"{GetClassName(entity)}\" => propertyName switch");
                generatedString.AppendLine("                {");
                foreach (var entityProp in entity.Properties.Items)
                {
                    generatedString.AppendLine($"                    \"{entityProp.Name}\" => true,");
                }
                generatedString.AppendLine("                    _ => false,");
                generatedString.AppendLine("                },");
            }
            generatedString.AppendLine("                _ => false,");
            generatedString.AppendLine("            };");
            generatedString.AppendLine("        }");
            generatedString.AppendLine("    }");
            generatedString.AppendLine("}");
            context.AddSource($"GraphQueryablePropertyService.{graph.Name}.g.cs", generatedString.ToString());
        }

        private static string GetClassName(Entity entity)
        {
            return entity switch
            {
                Node node => node.Name,
                Edge edge => $"{edge.Name}Edge",
                _ => throw new NotSupportedException("Entity type not supported"),
            };
        }

        private static void GenerateNode(GraphSchema graph, Node node, SourceProductionContext context)
        {
            var generatedString = new StringBuilder();
            generatedString.AppendLine("// <auto-generated/>");
            generatedString.AppendLine();
            generatedString.AppendLine("#nullable enable annotations");
            generatedString.AppendLine("#nullable disable warnings");
            generatedString.AppendLine();
            generatedString.AppendLine("// Suppress warnings about [Obsolete] member usage in generated code.");
            generatedString.AppendLine("#pragma warning disable CS0612, CS0618, CS9113, CS1591");
            generatedString.AppendLine();
            generatedString.AppendLine($"namespace {graph.Namespace}");
            generatedString.AppendLine("{");
            generatedString.AppendLine($"   public sealed class {node.Name}Order : global::GraphlessDB.INodeOrder");
            generatedString.AppendLine("   {");
            generatedString.AppendLine("        public global::GraphlessDB.OrderDirection? Id { get; set; }");
            foreach (var property in node.Properties.Items)
            {
                generatedString.AppendLine();
                generatedString.AppendLine($"        public global::GraphlessDB.OrderDirection? {property.Name} {{ get; set; }}");
            }
            generatedString.AppendLine("   }");
            generatedString.AppendLine();
            generatedString.AppendLine($"   public sealed class {node.Name}Filter : global::GraphlessDB.INodeFilter");
            generatedString.AppendLine("   {");
            generatedString.AppendLine("        public global::GraphlessDB.IdFilter? Id { get; set; }");
            foreach (var property in node.Properties.Items)
            {
                generatedString.AppendLine();
                generatedString.AppendLine($"        public global::GraphlessDB.{GetFilterType(graph, property.Type)}? {property.Name} {{ get; set; }}");
            }
            generatedString.AppendLine("   }");
            generatedString.AppendLine("}");
            generatedString.AppendLine();
            generatedString.AppendLine($"namespace GraphlessDB");
            generatedString.AppendLine("{");
            generatedString.AppendLine($"    public static partial class FluentGraphQueryExtensions");
            generatedString.AppendLine("    {");
            generatedString.AppendLine(
        @$"        public static global::GraphlessDB.FluentNodeQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{node.Name}> {node.Name}(
            this global::GraphlessDB.FluentGraphQuery<global::{graph.Namespace}.{graph.Name}> source, string id)
        {{
            return source.Node<global::{graph.Namespace}.{node.Name}>(id);
        }}

        public static global::GraphlessDB.FluentNodeOrDefaultQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{node.Name}> {node.Name}OrDefault(
            this global::GraphlessDB.FluentGraphQuery<global::{graph.Namespace}.{graph.Name}> source, string id)
        {{
            return source.NodeOrDefault<global::{graph.Namespace}.{node.Name}>(id);
        }}

        public static global::GraphlessDB.FluentNodeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{node.Name}> {node.Name}s(
            this global::GraphlessDB.FluentGraphQuery<global::{graph.Namespace}.{graph.Name}> source,
            global::{graph.Namespace}.{node.Name}Order? order = null,
            global::{graph.Namespace}.{node.Name}Filter? filter = null)
        {{
            return source.Nodes<global::{graph.Namespace}.{node.Name}>(c => c with {{ Order = order, Filter = filter }});
        }}
");
            generatedString.AppendLine("    }");
            generatedString.AppendLine("}");
            context.AddSource($"FluentGraphQueryExtensions.{node.Name}.g.cs", generatedString.ToString());
        }

        private static void GenerateEdge(GraphSchema graph, Edge edge, SourceProductionContext context)
        {
            var generatedString = new StringBuilder();
            generatedString.AppendLine("// <auto-generated/>");
            generatedString.AppendLine();
            generatedString.AppendLine("#nullable enable annotations");
            generatedString.AppendLine("#nullable disable warnings");
            generatedString.AppendLine();
            generatedString.AppendLine("// Suppress warnings about [Obsolete] member usage in generated code.");
            generatedString.AppendLine("#pragma warning disable CS0612, CS0618, CS9113, CS1591");
            generatedString.AppendLine();
            generatedString.AppendLine($"using GraphlessDB;");
            generatedString.AppendLine();
            generatedString.AppendLine($"namespace {graph.Namespace}");
            generatedString.AppendLine("{");
            GenerateEdgeOrder(edge, generatedString);
            generatedString.AppendLine();
            GenerateEdgeFilter(graph, edge, generatedString);
            generatedString.AppendLine("}");
            generatedString.AppendLine();
            generatedString.AppendLine($"namespace GraphlessDB");
            generatedString.AppendLine("{");
            GenerateFluentNodeQueryExtensions(graph, edge, generatedString, "FluentNodeQuery");
            generatedString.AppendLine();
            GenerateFluentNodeQueryExtensions(graph, edge, generatedString, "FluentNodeOrDefaultQuery");
            generatedString.AppendLine();
            GenerateFluentNodeConnectionQueryExtensions(graph, edge, generatedString);
            generatedString.AppendLine("}");
            context.AddSource($"FluentGraphQueryExtensions.{edge.Name}.g.cs", generatedString.ToString());
        }

        private static void GenerateEdgeFilter(GraphSchema graph, Edge edge, StringBuilder generatedString)
        {
            generatedString.AppendLine($"     public sealed class {edge.Name}EdgeFilter : global::GraphlessDB.IEdgeFilter");
            generatedString.AppendLine("     {");
            foreach (var property in edge.Properties.Items)
            {
                generatedString.AppendLine($"          public global::GraphlessDB.{GetFilterType(graph, property.Type)}? {property.Name} {{ get; set; }}");
                generatedString.AppendLine();
            }
            generatedString.AppendLine($"          public global::GraphlessDB.EdgeFilter? GetEdgeFilter()");
            generatedString.AppendLine($"          {{");
            generatedString.AppendLine($"               return new global::GraphlessDB.EdgeFilter(\"{edge.Name}\",\"{edge.In.NodeName}\",\"{edge.Out.NodeName}\", null, null,");
            generatedString.AppendLine($"                    global::System.Collections.Immutable.ImmutableList<global::GraphlessDB.ValueFilterItem>.Empty");
            foreach (var property in edge.Properties.Items)
            {
                generatedString.AppendLine($"                         .AddIf({property.Name} != null, new global::GraphlessDB.ValueFilterItem(\"{property.Name}\", {property.Name}))");
            }
            generatedString.AppendLine($"                    );");
            generatedString.AppendLine($"          }}");
            generatedString.AppendLine("     }");
        }

        private static void GenerateEdgeOrder(Edge edge, StringBuilder generatedString)
        {
            generatedString.AppendLine($"     public sealed class {edge.Name}EdgeOrder : global::GraphlessDB.IEdgeOrder");
            generatedString.AppendLine("     {");
            foreach (var property in edge.Properties.Items)
            {
                generatedString.AppendLine($"          public global::GraphlessDB.OrderDirection? {property.Name} {{ get; set; }}");
                generatedString.AppendLine();
            }
            generatedString.AppendLine("     }");
        }

        private static void GenerateFluentNodeConnectionQueryExtensions(GraphSchema graph, Edge edge, StringBuilder generatedString)
        {
            generatedString.AppendLine("     public static partial class FluentNodeConnectionQueryExtensions");
            generatedString.AppendLine("     {");
            generatedString.AppendLine($"          public static global::GraphlessDB.FluentEdgeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Name}Edge, global::{graph.Namespace}.{edge.In.NodeName}, global::{graph.Namespace}.{edge.Out.NodeName}> {edge.Out.FriendlyName}Edges(");
            generatedString.AppendLine($"               this global::GraphlessDB.FluentNodeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.In.NodeName}> source, global::{graph.Namespace}.{edge.Name}EdgeOrder? order = null, global::{graph.Namespace}.{edge.Name}EdgeFilter? filter = null)");
            generatedString.AppendLine("           {");
            generatedString.AppendLine($"               return source.InToEdges<global::{graph.Namespace}.{edge.Name}Edge, global::{graph.Namespace}.{edge.Out.NodeName}>(c => c with {{ Order = order, Filter = filter }});");
            generatedString.AppendLine("           }");
            generatedString.AppendLine();
            generatedString.AppendLine($"          public static global::GraphlessDB.FluentEdgeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Name}Edge, global::{graph.Namespace}.{edge.In.NodeName}, global::{graph.Namespace}.{edge.Out.NodeName}> {edge.In.FriendlyName}Edges(");
            generatedString.AppendLine($"               this global::GraphlessDB.FluentNodeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Out.NodeName}> source, global::{graph.Namespace}.{edge.Name}EdgeOrder? order = null, global::{graph.Namespace}.{edge.Name}EdgeFilter? filter = null)");
            generatedString.AppendLine("           {");
            generatedString.AppendLine($"               return source.OutToEdges<global::{graph.Namespace}.{edge.Name}Edge, global::{graph.Namespace}.{edge.In.NodeName}>(c => c with {{ Order = order, Filter = filter }});");
            generatedString.AppendLine("           }");
            generatedString.AppendLine();
            if (edge.In.NodeName == edge.Out.NodeName)
            {
                generatedString.AppendLine($"          public static global::GraphlessDB.FluentEdgeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Name}Edge, global::{graph.Namespace}.{edge.In.NodeName}, global::{graph.Namespace}.{edge.Out.NodeName}> {edge.In.FriendlyName}And{edge.Out.FriendlyName}Edges(");
                generatedString.AppendLine($"               this global::GraphlessDB.FluentNodeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Out.NodeName}> source, global::{graph.Namespace}.{edge.Name}EdgeOrder? order = null, global::{graph.Namespace}.{edge.Name}EdgeFilter? filter = null)");
                generatedString.AppendLine("           {");
                generatedString.AppendLine($"               return source.InAndOutToEdges<global::{graph.Namespace}.{edge.Name}Edge>(c => c with {{ Order = order, Filter = filter }});");
                generatedString.AppendLine("           }");
                generatedString.AppendLine();
            }
            generatedString.AppendLine($"          public static global::GraphlessDB.FluentNodeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Out.NodeName}> {edge.Out.FriendlyName}(");
            generatedString.AppendLine($"               this global::GraphlessDB.FluentNodeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.In.NodeName}> source, global::{graph.Namespace}.{edge.Name}EdgeOrder? order = null, global::{graph.Namespace}.{edge.Name}EdgeFilter? filter = null)");
            generatedString.AppendLine("          {");
            generatedString.AppendLine($"               return source.{edge.Out.FriendlyName}Edges(order, filter).OutFromEdges();");
            generatedString.AppendLine("          }");
            generatedString.AppendLine();
            generatedString.AppendLine($"          public static global::GraphlessDB.FluentNodeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.In.NodeName}> {edge.In.FriendlyName}(");
            generatedString.AppendLine($"               this global::GraphlessDB.FluentNodeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Out.NodeName}> source, global::{graph.Namespace}.{edge.Name}EdgeOrder? order = null, global::{graph.Namespace}.{edge.Name}EdgeFilter? filter = null)");
            generatedString.AppendLine("          {");
            generatedString.AppendLine($"               return source.{edge.In.FriendlyName}Edges(order, filter).InFromEdges();");
            generatedString.AppendLine("          }");
            if (edge.In.NodeName == edge.Out.NodeName)
            {
                generatedString.AppendLine($"          public static global::GraphlessDB.FluentNodeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.In.NodeName}> {edge.In.FriendlyName}And{edge.Out.FriendlyName}(");
                generatedString.AppendLine($"               this global::GraphlessDB.FluentNodeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Out.NodeName}> source, global::{graph.Namespace}.{edge.Name}EdgeOrder? order = null, global::{graph.Namespace}.{edge.Name}EdgeFilter? filter = null)");
                generatedString.AppendLine("          {");
                generatedString.AppendLine($"               return source.{edge.In.FriendlyName}And{edge.Out.FriendlyName}Edges(order, filter).InAndOutFromEdges();");
                generatedString.AppendLine("          }");
            }
            generatedString.AppendLine("     }");
        }

        private static void GenerateFluentNodeQueryExtensions(GraphSchema graph, Edge edge, StringBuilder generatedString, string sourceTypeName)
        {
            generatedString.AppendLine($"     public static partial class {sourceTypeName}Extensions");
            generatedString.AppendLine("     {");
            generatedString.AppendLine($"          public static global::GraphlessDB.FluentEdgeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Name}Edge, global::{graph.Namespace}.{edge.In.NodeName}, global::{graph.Namespace}.{edge.Out.NodeName}> {edge.Out.FriendlyName}Edges(");
            generatedString.AppendLine($"               this global::GraphlessDB.{sourceTypeName}<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.In.NodeName}> source, global::{graph.Namespace}.{edge.Name}EdgeOrder? order = null, global::{graph.Namespace}.{edge.Name}EdgeFilter? filter = null)");
            generatedString.AppendLine("           {");
            generatedString.AppendLine($"               return source.InToEdges<global::{graph.Namespace}.{edge.Name}Edge, global::{graph.Namespace}.{edge.Out.NodeName}>(c => c with {{ Order = order, Filter = filter }});");
            generatedString.AppendLine("           }");
            generatedString.AppendLine();
            generatedString.AppendLine($"          public static global::GraphlessDB.FluentEdgeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Name}Edge, global::{graph.Namespace}.{edge.In.NodeName}, global::{graph.Namespace}.{edge.Out.NodeName}> {edge.In.FriendlyName}Edges(");
            generatedString.AppendLine($"               this global::GraphlessDB.{sourceTypeName}<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Out.NodeName}> source, global::{graph.Namespace}.{edge.Name}EdgeOrder? order = null, global::{graph.Namespace}.{edge.Name}EdgeFilter? filter = null)");
            generatedString.AppendLine("           {");
            generatedString.AppendLine($"               return source.OutToEdges<global::{graph.Namespace}.{edge.Name}Edge, global::{graph.Namespace}.{edge.In.NodeName}>(c => c with {{ Order = order, Filter = filter }});");
            generatedString.AppendLine("           }");
            generatedString.AppendLine();
            if (edge.In.NodeName == edge.Out.NodeName)
            {
                generatedString.AppendLine($"          public static global::GraphlessDB.FluentEdgeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Name}Edge, global::{graph.Namespace}.{edge.In.NodeName}, global::{graph.Namespace}.{edge.Out.NodeName}> {edge.In.FriendlyName}And{edge.Out.FriendlyName}Edges(");
                generatedString.AppendLine($"               this global::GraphlessDB.{sourceTypeName}<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Out.NodeName}> source, global::{graph.Namespace}.{edge.Name}EdgeOrder? order = null, global::{graph.Namespace}.{edge.Name}EdgeFilter? filter = null)");
                generatedString.AppendLine("           {");
                generatedString.AppendLine($"               return source.InAndOutToEdges<global::{graph.Namespace}.{edge.Name}Edge>(c => c with {{ Order = order, Filter = filter }});");
                generatedString.AppendLine("           }");
                generatedString.AppendLine();
            }
            generatedString.AppendLine($"          public static global::GraphlessDB.FluentNodeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Out.NodeName}> {edge.Out.FriendlyName}(");
            generatedString.AppendLine($"               this global::GraphlessDB.{sourceTypeName}<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.In.NodeName}> source, global::{graph.Namespace}.{edge.Name}EdgeOrder? order = null, global::{graph.Namespace}.{edge.Name}EdgeFilter? filter = null)");
            generatedString.AppendLine("          {");
            generatedString.AppendLine($"               return source.{edge.Out.FriendlyName}Edges(order, filter).OutFromEdges();");
            generatedString.AppendLine("          }");
            generatedString.AppendLine();
            generatedString.AppendLine($"          public static global::GraphlessDB.FluentNodeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.In.NodeName}> {edge.In.FriendlyName}(");
            generatedString.AppendLine($"               this global::GraphlessDB.{sourceTypeName}<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Out.NodeName}> source, global::{graph.Namespace}.{edge.Name}EdgeOrder? order = null, global::{graph.Namespace}.{edge.Name}EdgeFilter? filter = null)");
            generatedString.AppendLine("          {");
            generatedString.AppendLine($"               return source.{edge.In.FriendlyName}Edges(order, filter).InFromEdges();");
            generatedString.AppendLine("          }");
            generatedString.AppendLine();
            if (edge.In.NodeName == edge.Out.NodeName)
            {
                generatedString.AppendLine($"          public static global::GraphlessDB.FluentNodeConnectionQuery<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.In.NodeName}> {edge.In.FriendlyName}And{edge.Out.FriendlyName}(");
                generatedString.AppendLine($"               this global::GraphlessDB.{sourceTypeName}<global::{graph.Namespace}.{graph.Name}, global::{graph.Namespace}.{edge.Out.NodeName}> source, global::{graph.Namespace}.{edge.Name}EdgeOrder? order = null, global::{graph.Namespace}.{edge.Name}EdgeFilter? filter = null)");
                generatedString.AppendLine("          {");
                generatedString.AppendLine($"               return source.{edge.In.FriendlyName}And{edge.Out.FriendlyName}Edges(order, filter).InAndOutFromEdges();");
                generatedString.AppendLine("          }");
            }
            generatedString.AppendLine("     }");
        }

        private static string GetFilterType(GraphSchema graph, string typeName)
        {
            if (IsValueType(typeName))
            {
                return $"{typeName}Filter";
            }

            if (IsEnumType(graph, typeName))
            {
                return $"EnumFilter<{typeName}>";
            }

            throw new InvalidOperationException($"NodePropertyType '{typeName}' not supported");
        }

        private static bool IsEnumType(GraphSchema graph, string typeName)
        {
            return graph.Enums.Items.Where(e => e.Name == typeName).Any();
        }

        private static bool IsValueType(string typeName)
        {
            return typeName switch
            {
                "Id" => true,
                "String" => true,
                "DateTime" => true,
                "Int" => true,
                _ => false,
            };
        }
    }
}
