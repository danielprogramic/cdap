/**
 * Namespace dropdown + breadcrumbs
 */

angular.module(PKG.name+'.commons')
.directive('myBreadcrumbs', function ($stateParams) {
  return {
    restrict: 'E',
    templateUrl: 'breadcrumbs/breadcrumbs.html',
    controller: 'breadcrumbsCtrl',
    scope: {
      hideNsDropdown: '=?'
    },
    link: function (scope, element, attrs) {
      scope.hideNsDropdown = attrs.hideNsDropdown === 'true';
      scope.element = element;
      scope.$stateParams = $stateParams;
    }
  };

});
